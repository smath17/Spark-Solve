import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, Strategy}
import org.apache.spark.sql.catalyst.expressions.{Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

case class MultiplyOptimizationRule(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case mul @ Multiply(left, right, false) if right.isInstanceOf[Literal] &&
      right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
      left
  }
}

//object NewStrategy extends Strategy {
//  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
//    case mul @ Multiply(left, right, false) if right.isInstanceOf[Literal] &&
//      right.asInstanceOf[Literal].value.asInstanceOf[Double] == 1.0 =>
//      left
//  }
//}

object suu {
  def main(args: Array[String]): Unit = {
    type ExtensionsBuilder = SparkSessionExtensions => Unit
    val extBuilder: ExtensionsBuilder = { e => e.injectOptimizerRule(MultiplyOptimizationRule) }

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[2]")
      //.withExtensions(extBuilder)
      .getOrCreate()


    val df = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ";")
      .load("examples/src/main/resources/people.csv")
    df.toDF.write.saveAsTable("person")

    //scalastyle:off
    println(spark.sql("select age * 1 from person").queryExecution.optimizedPlan
      .numberedTreeString)
    //scalastyle:on
    spark.stop
  }
}