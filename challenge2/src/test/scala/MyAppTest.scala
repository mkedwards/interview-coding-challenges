import org.scalatest.flatspec.AnyFlatSpec

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class MyAppTest extends AnyFlatSpec with MASharedSparkContext  {

  def readCSV(p:String) = spark.read.option("header",true).option("quote","\"").option("escape","\"").csv(p)

  behavior of "a MyApp"

  it should "test something" in {
    val basePath = "src/main/resources/"

    val df_movies  = readCSV(s"${basePath}tmdb_5000_movies.csv")
    val df_credits = readCSV(s"${basePath}tmdb_5000_credits.csv")

    val castSchema = ArrayType(
      new StructType()
        .add("order", IntegerType, true)
        .add("cast_id", IntegerType, true)
        .add("id", IntegerType, true)
        .add("gender", IntegerType, true)
        .add("name", StringType, true)
        .add("character", StringType, true)
        .add("credit_id", StringType, true)
      )

    val df_cast = df_credits
        .join(df_movies, df_credits("movie_id") === df_movies("id"), "leftouter")
        .withColumn("cast_member", explode(from_json(col("cast"), castSchema)).as("cast_member"))
        .select(df_credits("movie_id"), df_credits("title"), col("budget"), col("cast_member.*"))
        .repartition(200, col("id"))
        .sortWithinPartitions(col("id"), col("movie_id"), col("order"))
        .dropDuplicates("id", "movie_id")
        .filter(col("budget") > 999 && col("order") < 5)

    val df_avg_budget = df_cast.groupBy("id")
        .agg(first("name").as("a_name"), avg("budget").as("avg_budget"), count("movie_id").as("movie_count"), max("gender").as("max_gender"))
        .filter(col("movie_count") >= 5)
        .orderBy(col("avg_budget").desc)

    val df_big_cast = df_cast.join(df_avg_budget, df_cast("id") === df_avg_budget("id"), "inner")

    // df_cast.show(500, truncate = false)
    // df_big_cast.show(500, truncate = false)

    df_avg_budget.filter(col("max_gender") === 0).show(10, truncate = false)
    df_avg_budget.filter(col("max_gender") === 1).show(10, truncate = false)
    df_avg_budget.filter(col("max_gender") === 2).show(10, truncate = false)
  }
}
