import org.scalatest.flatspec.AnyFlatSpec

import com.ma.analytics.SparkFactory

import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

class MyAppTest extends AnyFlatSpec with MASharedSparkContext  {

  def readCSV(p:String) = spark.read.option("header",true).option("quote","\"").option("escape","\"").csv(p)

  behavior of "a MyApp"

  it should "read the lookup dataset and a daily file" in {
    val basePath = "src/main/resources"

    val df_movies_metadata  = readCSV(s"${basePath}/movies_metadata/movies_metadata.csv.gz")
    df_movies_metadata.show(5, truncate = false)

    import org.apache.spark.sql.types._
    val ratingsSchema = StructType(
      StructField("user_id", StringType, nullable = true) ::
        StructField("movie_id", StringType, nullable = true) ::
        StructField("rating", DoubleType, nullable = true) ::
        StructField("ts", LongType, nullable = true) :: Nil)

    val df_ratings = spark.read
      .option("header", false)
      .schema(schema = ratingsSchema)
      .csv(s"${basePath}/ratings/20171201/partition1/ratings.20171201.partition1.csv.gz")

    df_ratings.show(5, truncate = false)
    df_ratings.groupBy(col("movie_id"), col("user_id")).count.orderBy(col("count").desc).show(5, truncate = false)
  }

  it should "left outer join a daily file against the lookup dataset" in {
    val basePath = "src/main/resources"

    val df_movies_metadata  = readCSV(s"${basePath}/movies_metadata/movies_metadata.csv.gz")
    val renamedColumns = df_movies_metadata.columns.map(c => df_movies_metadata(c).as("movie_" + c))
    val df_movie_lookup = df_movies_metadata.select(renamedColumns: _*)
      .persist(StorageLevel.MEMORY_AND_DISK)

    import org.apache.spark.sql.types._
    val ratingsSchema = StructType(
      StructField("user_id", StringType, nullable = true) ::
        StructField("movie_id", StringType, nullable = true) ::
        StructField("rating", DoubleType, nullable = true) ::
        StructField("ts", LongType, nullable = true) :: Nil)

    val df_ratings_raw = spark.read
      .option("header", false)
      .schema(schema = ratingsSchema)
      .csv(s"${basePath}/ratings/*/*")
      .filter(col("rating").isNotNull && col("ts").isNotNull)

    val df_ratings = df_ratings_raw
      .repartition(200, col("movie_id"))
      .sortWithinPartitions(col("movie_id"), col("user_id"), col("ts").desc)
      .dropDuplicates("movie_id", "user_id")

    val df_ratings_avg = df_ratings
      .groupBy("movie_id")
      .agg(avg("rating").as("avg_movie_rating"), count("*").as("number_of_votes"))
      .persist(StorageLevel.DISK_ONLY)

    val df_ratings_joined = df_ratings_avg.join(broadcast(df_movie_lookup), df_ratings_avg("movie_id") === df_movie_lookup("movie_id"), "leftouter")
                                          .select(df_ratings_avg("movie_id"),
                                                  df_movie_lookup("movie_title"),
                                                  df_movie_lookup("movie_runtime"),
                                                  df_ratings_avg("avg_movie_rating"),
                                                  df_ratings_avg("number_of_votes"))

    df_ratings_raw.groupBy(col("movie_id"), col("user_id")).count.orderBy(col("count").desc).show(5, truncate = false)
    df_ratings.groupBy(col("movie_id"), col("user_id")).count.orderBy(col("count").desc).show(5, truncate = false)
    df_ratings_joined.orderBy(col("number_of_votes").desc, col("avg_movie_rating").desc, col("movie_id")).show(truncate = false)

    printf("Raw row count = %d\n", df_ratings_raw.count)
    printf("Deduplicated row count = %d\n", df_ratings.count)
    printf("Aggregated/joined row count = %d\n", df_ratings_joined.count)
    df_ratings_raw.agg(countDistinct("movie_id")).show(truncate = false)

    df_ratings_raw.filter(col("user_id") === 207971).orderBy(col("movie_id"), col("ts")).show(truncate = false)
    df_ratings.filter(col("user_id") === 207971).orderBy(col("movie_id"), col("ts")).show(truncate = false)

    df_ratings_joined.createOrReplaceTempView("df_ratings_joined")
    val like_sql = "SELECT /*+ COALESCE(1) */ * FROM df_ratings_joined WHERE avg_movie_rating >= 3.5 AND number_of_votes >= 3 AND lower(movie_title) LIKE 's%' ORDER BY avg_movie_rating DESC"
    val like_query = spark.sql(like_sql)

    printf("\n")
    printf("%s\n", like_sql)
    printf("\n")
    like_query.explain
    like_query.show(truncate = false)
  }

  it should "query against the global temp view" in {
    SparkFactory.initDailyRatings()
    SparkFactory.updateRatingsGlobalTempView("movies_metadata")

    val like_sql = "SELECT /*+ COALESCE(1) */ * FROM global_temp.ratings WHERE avg_movie_rating >= 3.5 AND number_of_votes >= 3 AND lower(movie_title) LIKE 's%' ORDER BY avg_movie_rating DESC"
    val like_query = spark.sql(like_sql)

    printf("%s\n", like_sql)
    printf("\n")
    like_query.explain
    like_query.show(truncate = false)
  }
}
