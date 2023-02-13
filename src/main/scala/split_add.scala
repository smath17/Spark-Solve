import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Add, Literal, Multiply}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class splittet_add(spark: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case add @ Add(left, right, false) if right.isInstanceOf[Literal] => left
  }
}


