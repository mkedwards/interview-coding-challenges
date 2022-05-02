//
// com.ma.analytics.SparkFactory
//

package com.ma.analytics

import java.io.File

import scala.collection.immutable._
import scala.math.Ordering.Implicits._
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

// An implicit Ordering on IdTuple keys is needed by RDD.repartitionAndSortWithinPartitions().
// See https://stackoverflow.com/a/19348339 for an explanation of the type parameterization ugliness.
//
object IdTuple {
  implicit def ordering[A <: IdTuple]: Ordering[A] =
    Ordering.by(id_tuple => (id_tuple.movie_id, id_tuple.user_id))
}

// TimestampedRating holds the value tuple for the RDD in which we reduce to the newest rating that a user has given a particular movie.
//
case class TimestampedRating(ts: Long, rating: Double)

// An implicit Ordering on TimestampedRating values is a convenient way to enforce associativity and commutativity in the
// function passed to PairRDDFunctions.reduceByKey().  With the help of scala.math.Ordering.Implicits._, it's just (_ max _).
//
object TimestampedRating {
  implicit def ordering[A <: TimestampedRating]: Ordering[A] =
    Ordering.by(tr => (tr.ts, tr.rating))
}

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

  // The schema for the daily ratings files is really only used to configure the full-featured CSV reader, which is plumbed
  // in through the DataFrameReader layer of Spark.  (We immediately discard the DataFrame model in favor of paired RDDs,
  // which are unfortunately the only sane way to induce consistent partition layout and avoid needless shuffles when forming
  // the union of daily data sets and reducing/aggregating them to average movie ratings.)  The reason why we disallow nullable
  // values up front at CSV ingestion time is that we want to use the low-level getString(), getDouble(), etc. column accessors
  // on Row, which don't handle null values gracefully.  (There isn't really a use case for null values in any of these columns
  // anyway, as contrasted with the movie metadata, where it's reasonable for any field other than movie_id to be null.)
  //
  private val dailyRatingsSchema = StructType(
    StructField("user_id", StringType, nullable = false) ::
      StructField("movie_id", StringType, nullable = false) ::
      StructField("rating", DoubleType, nullable = false) ::
      StructField("ts", LongType, nullable = false) :: Nil)

  // The schema for the average ratings DataFrame will be applied directly to an RDD[Row] by calling SparkSession.createDataFrame().
  // That's not the prettiest way to produce a DataFrame, but it's very nearly the only way to preserve partitioning by hash of
  // movie_id through the reduce/aggregate stages of computation.  None of the columns need to allow null values.
  //
  private val avgRatingsSchema = StructType(
    StructField("movie_id", StringType, nullable = false) ::
      StructField("avg_movie_rating", DoubleType, nullable = false) ::
      StructField("number_of_votes", LongType, nullable = false) :: Nil)

  // This is not a very Scala-ish way to express this; but the eventual goal is to be able to do thread-safe updates to the set of
  // daily RDDs that go into the construction of the aggregate ratings.  The choice of TreeMap is for the sake of the SortedMap
  // interface; a merge-sort-based implementation of reduceByKey on the union of multiple (co-partitioned, co-located, properly sorted)
  // RDDs might be expected to run slightly faster on real data at scale if the most recent data is first in the union.
  // Note that the TreeMap itself is immutable; the dailyRatings member is a mutable atomic reference to it.  Ref[IO].of() returns a
  // pure effect; we run it synchronously in order to seed the data member with a valid Ref at SparkFactory construction time.
  //
  private var dailyRatings = Ref[IO].of(
    new TreeMap[String, RDD[(IdTuple, TimestampedRating)]]()(implicitly[Ordering[String]].reverse)
  ).unsafeRunSync()

  // Ingest a (partitioned, compressed) daily CSV file to produce an RDD laid out for efficient union/reduceByKey in avgRatingsDF().
  // Note that RDD.repartitionAndSortWithinPartitions() relies on an implicit Ordering on the key.  Sorting down to the (movie_id, user_id)
  // level during the initial shuffle/partition by movie_id doesn't cost us much, and improves the locality of the later union/reduceByKey
  // operations on a set co-partitioned, co-located daily RDDs.  We don't go to the additional effort of sorting values by descending
  // timestamp, because multiple ratings of the same movie by the same user within the same daily file are going to be relatively rare.
  //
  // TODO persist this at DISK_ONLY level during tests, but DISK_ONLY_2 level in a production Spark cluster
  //
  private def dailyRatingsRDD(date: String): RDD[(IdTuple, TimestampedRating)] = {
    spark.read
      .option("header", false)
      .option("mode", "DROPMALFORMED")
      .option("ignoreLeadingWhiteSpace", true)
      .option("ignoreTrailingWhiteSpace", true)
      .schema(schema = dailyRatingsSchema)
      .csv(s"${basePath}/ratings/${date}/*")
      .rdd
      .map { row =>
        (IdTuple(row.getString(1), row.getString(0)), TimestampedRating(row.getLong(3), row.getDouble(2)))
      }
      .repartitionAndSortWithinPartitions(new IdPartitioner(partitionCount))
      .persist(StorageLevel.DISK_ONLY)
  }

  // loadDailyRatings(regEx) loads the CSV files for all dates that match the given regular expression into RDDs for use
  // in subsequent calls to updateRatingsGlobalTempView(metadataName).  The set of newly loaded RDDs is merged with the
  // results of previous calls to loadDailyRatings(), clobbering any existing RDD for a given date.  This is presently
  // a synchronous, sequential operation, but the design goal is that we should be able to do these loads concurrently
  // and call a notification callback on completion, and that it should be safe to call updateRatingsGlobalTempView()
  // at any point during this process.
  //
  def loadDailyRatings(regEx: Regex): Unit = {
    getListOfSubDirectories(new File(basePath + "/ratings"))
      .filter { case regEx() => true case _ => false }
      .foreach(date => {
        val newDaily = dailyRatingsRDD(date)
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

  // Iterator mapper that extracts the movie_id and rating fields of each (IdTuple, TimestampedRating) pair, dropping the user_id
  // and timestamp fields, which are no longer needed once the ratings for each (movie_id, user_id) tuple have been deduplicated.
  //
  // TODO this probably belongs either outside the SparkFactory object or inside the definition of avgRatingsDF()
  //
  private def extractRating(iter: Iterator[(IdTuple, TimestampedRating)]) : Iterator[(String, Double)] = {
    iter.map{case (id_tuple, rating) => (id_tuple.movie_id, rating.rating)}
  }

  // Iterator mapper that takes the ratio of total to count (to get the mean rating for the movie, which is what we want at query time)
  // and wraps a Row around the (movie_id, avg_movie_rating, number_of_votes) tuple (since we need an RDD[Row] to feed to createDataFrame).
  //
  // TODO this probably belongs either outside the SparkFactory object or inside the definition of avgRatingsDF()
  //
  private def toRow(iter: Iterator[(String, TotalRatings)]) : Iterator[Row] = {
    iter.map{case (movie_id, tr) => Row(movie_id, tr.total / tr.count, tr.count)}
  }

  // The avgRatingsDF() implementation is the heart of the application.  The reason why we've gone through all of the surrounding
  // gyrations and coded down to the RDD level is that it gives us detailed control of row partitioning and partition placement,
  // avoiding excess shuffles in the union/reduce/aggregate sequence.  You would think this would be an essential function of a
  // distributed query planner at the DataFrame level; but unless you know something I don't, as of Spark 3.2.1, this level of
  // control over partitioning is not a thing in Spark SQL.
  //
  // TODO persist this at MEMORY_AND_DISK level during tests, but MEMORY_AND_DISK_2 level in a production Spark cluster
  //
  private def avgRatingsDF(): DataFrame = {
    val rddsEffect = for {
      m <- dailyRatings.get
    } yield m.values
    val rdds = rddsEffect.unsafeRunSync()

    val avgRatingsRDD = rdds.reduce(_.union(_))
      //
      // Note that we enforce exact associativity/commutativity by using the implicit Ordering defined on TimestampedRating above,
      // in which the value of the rating is used as a tiebreaker for equal timestamps.
      //
      .reduceByKey(_ max _)
      //
      // We can set preservesPartitioning = true here because our custom IdPartitioner partitions only by hash of movie_id.
      //
      .mapPartitions(extractRating, preservesPartitioning = true)
      //
      // TODO It's kind of ugly for the seqOp and combOp functions to be open-coded here; they should probably be refactored into
      //      implicits on the TotalRatings case class.  Then again, making Numeric implicits work on a case class seems to be
      //      impossibly fiddly, so maybe we'll stick with the open-coded version.
      //
      .aggregateByKey(TotalRatings(0.0, 0))(
        (tr, rating) => TotalRatings(tr.total + rating, tr.count + 1),
        (tr1, tr2) => TotalRatings(tr1.total + tr2.total, tr1.count + tr2.count)
      )
      //
      // We set preservesPartitioning = true here, but it doesn't really matter because a RDD[Row] is not a paired RDD.
      // It would matter if we were trying to accomplish a partitioned join against a second large DataFrame, but the lookup
      // metadata is small enough that we coalesce it to one partition and broadcast it (after query-time filtering) to all
      // partitions.  Really the average ratings dataset is small enough, after the above aggregateByKey step, that we could
      // coalesce it to one partition; but there is perhaps some merit in demonstrating how wrapping an already-partitioned
      // RDD in a DataFrame works?
      //
      .mapPartitions(toRow, preservesPartitioning = true)

    // TODO either gate this with a global test/debug flag or use the spark.logLineage property on the SparkSession instead
    //
    println(avgRatingsRDD.toDebugString)

    spark.createDataFrame(avgRatingsRDD, avgRatingsSchema)
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  // Ingest the CSV for the movie metadata (lookup) table, assuming that the first line contains a header with column names.
  // Rename the columns with a prefix "movie_", so that they match the specification in the README for the lookup dataset.
  // No matter how the data is laid out initially, coalesce it to a single partition and sort by movie_id, so that it can
  // be efficiently filtered and broadcast-joined against (partitioned, sorted) ratings data at query time.
  //
  // TODO tolerate column names in header with or without "movie_" prefix
  //
  // TODO convince Spark that there are no nulls in the movie_id column to avoid needless isnotnull(movie_id) checks at query time
  //
  // TODO persist this at MEMORY_AND_DISK level during tests, but MEMORY_AND_DISK_2 level in a production Spark cluster
  //
  private def movieLookupDF(metadataName: String): DataFrame = {
    val df_movies_metadata = spark.read
      .option("header", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .csv(s"${basePath}/${metadataName}/${metadataName}.csv.gz")

    val renamedColumns = df_movies_metadata.columns.map(c => df_movies_metadata(c).as("movie_" + c))

    df_movies_metadata.select(renamedColumns: _*)
      .coalesce(1)
      .sortWithinPartitions(col("movie_id"))
      .persist(StorageLevel.MEMORY_AND_DISK_2)
  }

  // updateRatingsGlobalTempView(metadataName) regenerates the average ratings DataFrame based on the current set of
  // daily ratings RDDs, regenerates the lookup (movie metadata) dataframe based on the current CSV (specified by
  // metadataName), and updates the global temp view based on a (left outer) join of these two DataFrames.  The intention
  // is that any caching logic for partial roll-ups of daily RDDs, etc. will be encapsulated within the implementation of
  // updateRatingsGlobalTempView().  Again, this is presently a synchronous, sequential operation, but the design goal is
  // that we should be able to do these loads concurrently and call a notification callback on completion.
  //
  // TODO the column order is currently adjusted for display purposes using a brute-force select; rewrite this so that it
  //      doesn't have to know the list of columns present in the metadata file
  //
  def updateRatingsGlobalTempView(metadataName: String): Unit = {
    val df_avg_ratings = avgRatingsDF()
    val df_movie_lookup = movieLookupDF(metadataName)

    val df_ratings_joined = df_avg_ratings.join(broadcast(df_movie_lookup), df_avg_ratings("movie_id") === df_movie_lookup("movie_id"), "leftouter")
                                          .select(df_avg_ratings("movie_id"),
                                                  df_movie_lookup("movie_title"),
                                                  df_movie_lookup("movie_runtime"),
                                                  df_avg_ratings("avg_movie_rating"),
                                                  df_avg_ratings("number_of_votes"))

    df_ratings_joined.createOrReplaceGlobalTempView("ratings")
    ()
  }
}
