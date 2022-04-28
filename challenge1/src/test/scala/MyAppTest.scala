import org.scalatest.FlatSpec

class MyAppTest extends FlatSpec with MASharedSparkContext  {

  def readCSV(p:String) = spark.read.option("header",true).option("quote","\"").option("escape","\"").csv(p)

  "a MyApp" should "test something" in {
    val basePath = "src/main/resources/"

    val df_movies_metadata  = readCSV(s"${basePath}/movies_metadata/movies_metadata.csv.gz")
    df_movies_metadata.show(5, truncate = false)

    import org.apache.spark.sql.types._
    val ratingsSchema = StructType(
      StructField("user_id", StringType, nullable = true) ::
        StructField("movie_id", StringType, nullable = true) ::
        StructField("ratnig", DoubleType, nullable = true) ::
        StructField("ts", LongType, nullable = true) :: Nil)

    val df_ratings = spark.read
      .format("org.apache.spark.csv")
        .option("header", false)
      .schema(schema = ratingsSchema)
      .csv(s"${basePath}/ratings/20171201/partition1/ratings.20171201.partition1.csv.gz")

    df_ratings.show(5, truncate = false)


  }
}
