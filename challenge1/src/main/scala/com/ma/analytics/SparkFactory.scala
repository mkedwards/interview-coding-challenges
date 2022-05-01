package com.ma.analytics

import java.io.File

import scala.collection.mutable._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.Partitioner

case class IdTuple(movie_id: String, user_id: String)

case class Rating(ts: Long, rating: Double)

case class TotalRatings(total: Double, count: Long)

class IdPartitioner(numberOfPartitions: Int) extends Partitioner {
  override def numPartitions: Int = numberOfPartitions

  override def getPartition(key: Any): Int = key match {
    case movie_id: String => Math.abs(movie_id.hashCode() % numPartitions)
    case id_tuple: IdTuple => Math.abs(id_tuple.movie_id.hashCode() % numPartitions)
    case row: Row => Math.abs(row.getString(0).hashCode() % numPartitions)
    case _ => 0
  }

  // Java equals method to let Spark compare our Partitioner objects
  override def equals(other: Any): Boolean = other match {
    case partitioner: IdPartitioner =>
      partitioner.numPartitions == numPartitions
    case _ =>
      false
  }
}

object SparkFactory {
  private val spark = SparkSession.builder
    .master("local[2]")
    .appName("spark-code-challenge")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate

  private val basePath = "src/main/resources"

  private def getListOfSubDirectories(dir: File): List[String] =
      dir.listFiles
         .filter(_.isDirectory)
         .map(_.getName)
         .toList

  private val partitionCount = 200

  private def lookupDF(baseName: String): DataFrame = {
    val df_movies_metadata = spark.read
      .option("header", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"${basePath}/${baseName}/${baseName}.csv.gz")
      .coalesce(1)
    val renamedColumns = df_movies_metadata.columns.map(c => df_movies_metadata(c).as("movie_" + c))
    df_movies_metadata.select(renamedColumns: _*)
      .persist(StorageLevel.MEMORY_AND_DISK_2)
  }

  private val ratingsSchema = StructType(
    StructField("user_id", StringType, nullable = false) ::
      StructField("movie_id", StringType, nullable = false) ::
      StructField("rating", DoubleType, nullable = false) ::
      StructField("ts", LongType, nullable = false) :: Nil)

  private val avgRatingsSchema = StructType(
    StructField("movie_id", StringType, nullable = false) ::
      StructField("avg_movie_rating", DoubleType, nullable = false) ::
      StructField("number_of_votes", LongType, nullable = false) :: Nil)

  private def ratingsRDD(date: String): RDD[(IdTuple, Rating)] = {
    spark.read
      .option("header", false)
      .option("mode", "DROPMALFORMED")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .schema(schema = ratingsSchema)
      .csv(s"${basePath}/ratings/${date}/*")
      .rdd
      .map { row =>
        (IdTuple(row.getString(1), row.getString(0)), Rating(row.getLong(3), row.getDouble(2)))
      }
      .partitionBy(new IdPartitioner(partitionCount))
      .persist(StorageLevel.DISK_ONLY)
  }

  // This is not a very Scala-ish way to express this; but the eventual goal is to be able to do thread-safe
  // updates to the set of daily RDDs that go into the construction of the aggregate ratings.
  // The bare TreeMap is a stand-in for what will eventually be a parallel RDD loader with a SortedMap interface.
  private var dailyRatings = new TreeMap[String, RDD[(IdTuple, Rating)]]()(implicitly[Ordering[String]].reverse)

  def initDailyRatings(): Unit = {
    getListOfSubDirectories(new File(basePath + "/ratings")).foreach(date => dailyRatings += (date -> ratingsRDD(date)))
    ()
  }

  private def newer(a:Rating, b:Rating) = if (b.ts > a.ts) b else a

  private def extractRating(iter: Iterator[(IdTuple, Rating)]) : Iterator[(String, Double)] = {
    iter.map{case (id_tuple, rating) => (id_tuple.movie_id, rating.rating)}
  }

  private def toRow(iter: Iterator[(String, TotalRatings)]) : Iterator[Row] = {
    iter.map{case (movie_id, tr) => Row(movie_id, tr.total / tr.count, tr.count)}
  }

  private def avgRatingsDF(): DataFrame = {
    val avgRatingsRDD = dailyRatings.values.reduce(_.union(_))
      .reduceByKey(newer)
      .mapPartitions(extractRating, preservesPartitioning = true)
      .aggregateByKey(new TotalRatings(0.0, 0))(
        (tr, rating) => new TotalRatings(tr.total + rating, tr.count + 1),
        (tr1, tr2) => new TotalRatings(tr1.total + tr2.total, tr1.count + tr2.count)
      )
      .mapPartitions(toRow, preservesPartitioning = true)

    println(avgRatingsRDD.toDebugString)

    spark.createDataFrame(avgRatingsRDD, avgRatingsSchema)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  def updateRatingsGlobalTempView(baseName: String): Unit = {
    val df_movie_lookup = lookupDF(baseName)
    val df_ratings_avg = avgRatingsDF()

    val df_ratings_joined = df_ratings_avg.join(broadcast(df_movie_lookup), df_ratings_avg("movie_id") === df_movie_lookup("movie_id"), "leftouter")
                                          .select(df_ratings_avg("movie_id"),
                                                  df_movie_lookup("movie_title"),
                                                  df_movie_lookup("movie_runtime"),
                                                  df_ratings_avg("avg_movie_rating"),
                                                  df_ratings_avg("number_of_votes"))

    df_ratings_joined.createOrReplaceGlobalTempView("ratings")
    ()
  }
}
