import org.apache.spark.sql.{Dataset, Encoder, Row}
import org.apache.spark.sql.expressions.Aggregator

case class order()
case class PaProblem(db: order, obj: String, constraint: Seq[String])
case class result(item: order, keep: Boolean)

object SolveUDAF extends Aggregator[order, PaProblem, Row] {
  override def zero: Nothing = ???

  override def reduce(b: Nothing, a: Any): Nothing = ???

  override def merge(b1: Nothing, b2: Nothing): Nothing = ???

  override def finish(reduction: Nothing): Nothing = ???

  override def bufferEncoder: Encoder[Nothing] = ???

  override def outputEncoder: Encoder[Nothing] = ???
}
