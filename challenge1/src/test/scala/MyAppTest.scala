import org.scalatest.flatspec.AnyFlatSpec

import org.apache.spark.sql.functions.col

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

    import org.apache.spark.sql.types._
    val ratingsSchema = StructType(
      StructField("user_id", StringType, nullable = true) ::
        StructField("movie_id", StringType, nullable = true) ::
        StructField("rating", DoubleType, nullable = true) ::
        StructField("ts", LongType, nullable = true) :: Nil)

    val df_ratings_raw = spark.read
      .format("csv")
      .option("header", false)
      .schema(schema = ratingsSchema)
      .csv(s"${basePath}/ratings/*/*")

    val df_ratings = df_ratings_raw
      .repartition(200, col("movie_id"))
      .sort(col("movie_id"), col("user_id"), col("ts").desc)
      .dropDuplicates("movie_id", "user_id")

    val df_ratings_joined = df_ratings.join(df_movie_lookup, df_ratings("movie_id") === df_movie_lookup("movie_id"), "leftouter")
                                      .select(df_ratings("movie_id"),
                                              df_movie_lookup("movie_title"),
                                              df_movie_lookup("movie_runtime"),
                                              df_ratings("user_id"),
                                              df_ratings("ts"),
                                              df_ratings("rating"))

    df_ratings_joined.show(5, truncate = false)
    df_ratings_raw.groupBy(col("movie_id"), col("user_id")).count.orderBy(col("count").desc).show(5, truncate = false)
    df_ratings_joined.groupBy(col("movie_id"), col("user_id")).count.orderBy(col("count").desc).show(5, truncate = false)
    printf("Raw row count = %d\n", df_ratings_raw.count)
    printf("Deduplicated/joined row count = %d\n", df_ratings.count)
    df_ratings_raw.filter(col("user_id") === 207971).orderBy(col("movie_id"), col("ts")).show(truncate = false)
    df_ratings_joined.filter(col("user_id") === 207971).orderBy(col("movie_id"), col("ts")).show(truncate = false)
  }
}
