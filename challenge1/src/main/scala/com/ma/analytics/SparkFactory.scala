package com.ma.analytics

import java.io.File

import scala.collection.immutable._
import scala.util.matching.Regex

import cats.effect.{IO, Ref, Sync}
import cats.syntax.all._

// For present purposes, we're using unsafeRunSync() with an implicit IORuntime, rather than a proper cats.effect.IOApp.
import cats.effect.unsafe.implicits.global

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel

import org.apache.spark.Partitioner

// IdTuple holds the key tuple for the RDD in which we reduce to the newest rating that a user has given a particular movie.
//
case class IdTuple(movie_id: String, user_id: String)

object IdTuple {
  implicit def ordering[A <: IdTuple]: Ordering[A] =
    Ordering.by(id_tuple => (id_tuple.movie_id, id_tuple.user_id))
}

// TimestampedRating holds the value tuple for the RDD in which we reduce to the newest rating that a user has given a particular movie.
//
case class TimestampedRating(ts: Long, rating: Double)

// TotalRatings holds the value tuple for the RDD into which we aggregate all users' ratings of a particular movie.
//
case class TotalRatings(total: Double, count: Long)

// The IDPartitioner is sensitive only to the movie_id portion of the key, which should be of one of three types:
//   * a bare string, which is how the paired RDD that holds per-movie aggregation results is keyed;
//   * an instance of IdTuple, which is how the paired RDD that holds per-movie, per-user timestamped ratings is keyed;
//   * an instance of Row, in case the partitioner gets applied in an RDD[Row] context; this is not how the DataFrame-level
//     repartition() method works internally, though arguably it should, and including this case has some future-proofing
//     value given that we use mapPartitions(toRow, preservesPartitioning = true) when converting to an RDD[Row].
//
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

// SparkFactory may not be the right name for this any more, now that it has grown most of the application function?
// but this is where the machinery around building RDDs and DataFrames is encapsulated.  It has two public members:
//
//   * loadDailyRatings(regEx) loads the CSV files for all dates that match the given regular expression into RDDs for use
//     in subsequent calls to updateRatingsGlobalTempView(metadataName).  The set of newly loaded RDDs is merged with the
//     results of previous calls to loadDailyRatings(), clobbering any existing RDD for a given date.  This is presently
//     a synchronous, sequential operation, but the design goal is that we should be able to do these loads concurrently
//     and call a notification callback on completion, and that it should be safe to call updateRatingsGlobalTempView()
//     at any point during this process.
//
//   * updateRatingsGlobalTempView(metadataName) regenerates the average ratings DataFrame based on the current set of
//     daily ratings RDDs, regenerates the lookup (movie metadata) dataframe based on the current CSV (specified by
//     metadataName), and updates the global temp view based on a (left outer) join of these two DataFrames.  The intention
//     is that any caching logic for partial roll-ups of daily RDDs, etc. will be encapsulated within the implementation of
//     updateRatingsGlobalTempView().  Again, this is presently a synchronous, sequential operation, but the design goal is
//     that we should be able to do these loads concurrently and call a notification callback on completion.
//
object SparkFactory {
  // TODO Plumb the partitionCount setting through to the SparkFactory constructor
  //
  private val partitionCount = 200

  // TODO Plumb the SparkSession builder parameters through to the SparkFactory constructor
  //
  // Note that the SparkFactory instance owns its own private SparkSession, but that the core builder parameters (including
  // spark.sql.shuffle.partitions) must match the other SparkSessions in the application, so that we can access the global
  // temp view that the SparkFactory maintains (and there aren't needless shuffles at query time).
  //
  private val spark = SparkSession.builder
    .master("local[2]")
    .appName("spark-code-challenge")
    .config("spark.sql.shuffle.partitions", partitionCount)
    .getOrCreate

  // TODO Plumb the basePath setting (or whatever logic is needed to find the input CSVs in a real Spark cluster) through
  // to the SparkFactory constructor
  //
  private val basePath = "src/main/resources"

  // TODO this probably belongs in a utility module
  //
  private def getListOfSubDirectories(dir: File): List[String] =
      dir.listFiles
         .filter(_.isDirectory)
         .map(_.getName)
         .toList

  private def lookupDF(metadataName: String): DataFrame = {
    val df_movies_metadata = spark.read
      .option("header", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"${basePath}/${metadataName}/${metadataName}.csv.gz")
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

  private def ratingsRDD(date: String): RDD[(IdTuple, TimestampedRating)] = {
    spark.read
      .option("header", false)
      .option("mode", "DROPMALFORMED")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .schema(schema = ratingsSchema)
      .csv(s"${basePath}/ratings/${date}/*")
      .rdd
      .map { row =>
        (IdTuple(row.getString(1), row.getString(0)), TimestampedRating(row.getLong(3), row.getDouble(2)))
      }
      .repartitionAndSortWithinPartitions(new IdPartitioner(partitionCount))
      .persist(StorageLevel.DISK_ONLY)
  }

  // This is not a very Scala-ish way to express this; but the eventual goal is to be able to do thread-safe
  // updates to the set of daily RDDs that go into the construction of the aggregate ratings.
  // The bare TreeMap is a stand-in for what will eventually be a parallel RDD loader with a SortedMap interface.
  // Note that the TreeMap itself is immutable; the dailyRatings member is a mutable atomic reference to it.
  private var dailyRatings
    = Ref[IO].of(new TreeMap[String, RDD[(IdTuple, TimestampedRating)]]()(implicitly[Ordering[String]].reverse)).unsafeRunSync()

  def loadDailyRatings(regEx: Regex): Unit = {
    getListOfSubDirectories(new File(basePath + "/ratings"))
      .filter { case regEx() => true case _ => false }
      .foreach(date => {
        val newDaily = ratingsRDD(date)
        val doUpdate = for {
          oldDaily <- dailyRatings.modify { m => (m + (date -> newDaily), m.get(date)) }
        } yield oldDaily match {
          // Note that there is a race condition between unpersisting a stale daily RDD here and a possible pending use of it
          // in the body of avgRatingsDF().  This is not an actual integrity problem, because the RDD definition remains valid;
          // and the persistence of the DataFrame returned by avgRatingsDF() ensures that this race will happen rarely enough that
          // it's probably not worth inserting refcounting mechanics to close it.
          case Some(rdd) => { rdd.unpersist(); () }
          case None => ()
        }
        doUpdate.unsafeRunSync()
      })
    ()
  }

  private def newer(a:TimestampedRating, b:TimestampedRating) = if (b.ts > a.ts) b else a

  private def extractRating(iter: Iterator[(IdTuple, TimestampedRating)]) : Iterator[(String, Double)] = {
    iter.map{case (id_tuple, rating) => (id_tuple.movie_id, rating.rating)}
  }

  private def toRow(iter: Iterator[(String, TotalRatings)]) : Iterator[Row] = {
    iter.map{case (movie_id, tr) => Row(movie_id, tr.total / tr.count, tr.count)}
  }

  private def avgRatingsDF(): DataFrame = {
    val rdds = for {
      m <- dailyRatings.get
    } yield m.values

    val avgRatingsRDD = rdds.unsafeRunSync().reduce(_.union(_))
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

  def updateRatingsGlobalTempView(metadataName: String): Unit = {
    val df_ratings_avg = avgRatingsDF()
    val df_movie_lookup = lookupDF(metadataName)

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
