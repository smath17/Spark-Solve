import org.apache.spark.solver.solver.DatasetSolve
import org.apache.spark.solver.{Maximize, PaProblem, UnknownVariableCol}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object TestQuery {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    // Decision variables
    val oKeep = new UnknownVariableCol("O_keep", 0, 1)
    val cKeep = new UnknownVariableCol("C_keep", 0, 1)

    // Constraints
    val withinBudget = "SUM(O_keep * O_TOTALPRICE) <= 10000"
    val maxCustomers = "SUM(C_keep) <= 3"

    // PA Query
    val query = new PaProblem(Seq(oKeep, cKeep), Maximize, "SUM(O_keep * O_TOTALPRICE) + SUM(C_keep * C_ACCTBAL)", Seq(withinBudget, maxCustomers))



    // sc.textFile("C:/Users/the_p/OneDrive/Skrivebord/tpc-data/tpc-data/test/*")
    val ordersDF = spark.read.format("csv").option("delimiter", "|").option("header", true).option("inferSchema", true).load("C:/Users/the_p/OneDrive/Skrivebord/tpc-data/tpc-data/test/orders.tbl")
    val customersDF = spark.read.format("csv").option("delimiter", "|").option("header", true).option("inferSchema", true).load("C:/Users/the_p/OneDrive/Skrivebord/tpc-data/tpc-data/test/customer.tbl")
    // val df = spark.read.format("csv").option("delimiter", "|").load("C:/Users/the_p/OneDrive/Skrivebord/tpc-data/tpc-data/test/*")
    // val tpcDF = ordersDF.join(customersDF, ordersDF("CUSTKEY") <=> customersDF("CUSTKEY"), "outer")
    val tpcDF = ordersDF.join(customersDF, "CUSTKEY")
    // tpcDF.write.format("csv").save("C:/Users/the_p/OneDrive/Skrivebord/tpc-data/tpc-data/test/testTable")

    tpcDF.show(10)

    tpcDF.findSolution(query, sc)


  }

}
