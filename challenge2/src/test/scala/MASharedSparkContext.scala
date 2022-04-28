import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql._
import org.scalatest.Suite

/*
 * Extends the original test library with sqlContext and sparkSession;
 * also, set warehouse dir so spark-warehouse dir doesn't show in the users directory anymore
 */
trait MASharedSparkContext extends SharedSparkContext {
  self: Suite =>

  lazy val sqlContext = new SQLContext(sc)
  lazy val spark = sqlContext.sparkSession

  override def conf = super.conf.set("spark.sql.warehouse.dir", "/tmp")

}
