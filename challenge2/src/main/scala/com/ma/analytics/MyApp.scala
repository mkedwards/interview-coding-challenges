package com.ma.analytics

import org.apache.spark.sql.SparkSession

object MyApp {
  def main(args:Array[String]) = {
    // create spark context
    val spark = SparkSession.builder().
      appName("spark-code-challenge").master("local[2]").getOrCreate()



  }

}
