MKE response to Challenge 1
=======================
Progress as of 2022-05-01
--------------
The implementation more or less meets specification, though it is not production-ready (it's still hard-coded to use the local Spark master
and data resources embedded in the assembly JAR) and doesn't have detailed tests.  There are a few test cases that show stages of implementation,
but they simply dump output to the console for inspection rather than verifying specific average ratings values.

NOTE that the test data itself has been tweaked to exercise cases where a rating is clobbered by a rating with a later timestamp!  If you look
carefully at the test output, you will see that different average ratings are reported for "Say Anything..." in test cases where the third daily
CSV has and hasn't been ingested.  Check the final test case in `src/test/scala/MyAppTest.scala`, and observe that `SparkFactory.loadDailyRatings()`
takes a regular expression parameter to specify which daily data sets it should load.

There is one notable deviation from the intermediate discussion with Wesley; the implementation disallows nulls in all columns of the daily CSVs.
As noted in comments in the code, allowing nulls in ratings would not be particularly useful (when would a null movie ID or user ID be valid, let
alone a null timestamp or rating?), and would cost performance and code complexity in the inner loop of conversion from DataFrame to pair RDD.

There are extensive comments in `com.ma.analytics.SparkFactory` describing the implementation strategy, including the use of RDD-level APIs to
accomplish the union/reduce/aggregate sequence without needless shuffle/repartition stages.  It's not *pretty* code, but it seems to get the job
done.  In addition to the test cases (which can be run as usual via `sbt test`), there is a sample application which can be rolled up along with
the test data via `sbt assembly` and run on a local Spark master.  The incantation for launching it (assuming that you have a Spark master at
`spark://localhost:7077`) is:
```
spark-submit --class com.ma.analytics.MyApp --master spark://localhost:7077 target/scala-2.12/spark-code-challenge-assembly-0.1.jar "avg_movie_rating >= 3.5 AND number_of_votes >= 3 AND lower(movie_title) LIKE 's%'"
```

Here the final string argument becomes `args(0)` of the `main` method of `MyApp`, and is used as the `WHERE` clause for a test query whose result
is printed to the console after the application has launched, loaded the test data, and set up the global temp view against which queries can be
performed.  (For the duration of the application, any `SparkSession` on the cluster that uses the same `appName` can query against this global
temp view.  Code paths exist in the SparkFactory class for refreshing the view when individual daily CSVs are added/replaced or when the movie
metadata CSV is updated, but there is presently no control path through which to invoke them.)

MyApp exposes an HTTP service on port 8088 to demonstrate running queries against the global temp view.  This service stays up until the app exits
(which happens when it receives a RETURN keystroke in the terminal where `spark-submit` is running; this is an automagical feature of Akka HttpApp).
For example, if the app is running on `localhost`, this link should return valid JSON with a list of six movies (some of them rather dubious!)
whose titles start with S:

http://localhost:8088/query?where=avg_movie_rating%20%3E%3D%203.5%20AND%20number_of_votes%20%3E%3D%205%20AND%20lower%28movie_title%29%20LIKE%20%27s%25%27

For reference, if you have installed Spark 3.2.1 using Homebrew on a Mac, this is how you get a local Spark master and worker running:

```
/usr/local/Cellar/apache-spark/3.2.1/libexec/sbin/start-master.sh -h localhost
/usr/local/Cellar/apache-spark/3.2.1/libexec/sbin/start-worker.sh spark://localhost:7077

```


Data Coding Challenge
=======================
Design and create a Apache Spark job using this skeleton that will process a provided daily file and build a dataset calculating average movie ratings as of the latest file received and includes all previous votes.

Data
--------------
Data is in src/main/resources.

## Requirements
The operations we expect to see:
- Processing
  - Only the latest movie rating by a user should be used/counted.
  - Assume that daily files are ~2TB and lookup data is about 1MB.
  - Resultant dataset to be read performant/optimized for querying.
- Metadata join
  - Metadata can be updated at any time and the resultant dataset needs to only show the latest runtime and title.
  - Metadata may not always have the respective movie listed.
- Reprocessing
  - Reprocessing of any specific days, i.e. We should be able to reprocess any one day's file with minimally added performance hit.
- Operational
  - Job execution time, how long does the job take.
- Technologies used:
  - Scala
  - Apache Spark.


### Daily file format:
- Tab delimited file with the following columns:
  - user_id
  - movie_id
  - rating (1-5)
  - voting-timestamp (Epoch)

### Lookup dataset:
- Comma delimited file with the following columns:
  - movie_id
  - movie_runtime
  - movie_title


### Final output columns:
- movie_id
- movie_title
- movie_runtime
- avg(movie_rating)
- number_of_votes

## Checking Out the Project
The project is hosted here on Gitlab.com.

## Submitting the Project
You will also receive an invitation to your own private repository on gitlab.com. Push your code to that repository as
you go. Instructions are provided by gitlab.com on how to push your code to a new repo.

## Challenge Duration
Average time to complete this task is 2 hours.
