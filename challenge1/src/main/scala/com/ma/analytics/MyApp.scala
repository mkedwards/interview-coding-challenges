package com.ma.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object MyApp {
  def main(args:Array[String]) = {
    SparkFactory.initDailyRatings()
    SparkFactory.updateRatingsGlobalTempView("movies_metadata")

    val spark = SparkSession.builder
      .master("local[2]")
      .appName("spark-code-challenge")
      .config("spark.sql.shuffle.partitions", "200")
      .getOrCreate

    val like_sql = "SELECT /*+ COALESCE(1) */ * FROM global_temp.ratings WHERE avg_movie_rating >= 3.5 AND number_of_votes >= 3 AND lower(movie_title) LIKE 's%' ORDER BY avg_movie_rating DESC"
    val like_query = spark.sql(like_sql)

    printf("%s\n", like_sql)
    printf("\n")
    like_query.explain
    like_query.show(truncate = false)
  }
}
