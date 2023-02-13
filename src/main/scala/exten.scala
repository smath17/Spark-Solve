import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser


lazy val spark = {
  val conf = new SparkConf().setAppName("example").setMaster("local[1]")
  type ExtensionsBuilder = SparkSessionExtensions => Unit
  def create(builder: ExtensionsBuilder): ExtensionsBuilder = builder
  val extension = create { extensions =>
    extensions.injectParser((_, _) => SqlBaseParser)
  }
  val session = SparkSession.builder().config(conf).withExtensions(extension).getOrCreate()
  session.sparkContext.setLogLevel("ERROR")
  session
}