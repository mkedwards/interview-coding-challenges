package com.ma.analytics

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.{HttpApp, Route}

object WebServer extends HttpApp {
  val spark = SparkSession.builder
    .master("local[2]")
    .appName("spark-code-challenge")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate

  override def routes: Route = {
    pathEndOrSingleSlash {
      get {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1>Hello World!! This is Akka responding..</h1>"))
      }
    } ~
    path("query") {
      parameter('where.as[String]) { whereClause =>
        complete(HttpEntity(ContentTypes.`application/json`, {
          val statement = s"SELECT /*+ COALESCE(1) */ * FROM global_temp.ratings WHERE ${whereClause} ORDER BY avg_movie_rating DESC"
          val result = spark.sql(statement).toJSON.collect
          "[\n  " + result.mkString(",\n  ") + "\n]"
        }))
      }
    }
  }
}

object MyApp {
  val spark = SparkSession.builder
    .master("local[2]")
    .appName("spark-code-challenge")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate

  def main(args:Array[String]) = {
    SparkFactory.loadDailyRatings(".*".r)
    SparkFactory.updateRatingsGlobalTempView("movies_metadata")

    val whereClause = args(0)
    val like_sql = s"SELECT /*+ COALESCE(1) */ * FROM global_temp.ratings WHERE ${whereClause} ORDER BY avg_movie_rating DESC"
    val like_query = spark.sql(like_sql)

    printf("%s\n", like_sql)
    printf("\n")
    like_query.explain
    like_query.show(truncate = false)

    WebServer.startServer("localhost", 8088)
  }
}
