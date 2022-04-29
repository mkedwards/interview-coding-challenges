import org.scalatest.flatspec.AnyFlatSpec

class MyAppTest extends AnyFlatSpec with MASharedSparkContext  {

  def readCSV(p:String) = spark.read.option("header",true).option("quote","\"").option("escape","\"").csv(p)

  behavior of "a MyApp"

  it should "test something" in {
    val basePath = "src/main/resources/"

    val df_movies  = readCSV(s"${basePath}tmdb_5000_movies.csv")
    val df_credits = readCSV(s"${basePath}tmdb_5000_credits.csv")

    df_movies.show()
    df_credits.show()
  }
}
