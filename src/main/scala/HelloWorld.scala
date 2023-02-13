import optimus.algebra.{Constraint, Expression}
import optimus.optimization._
import optimus.optimization.enums.SolverLib
import optimus.optimization.model.{MPFloatVar, ModelSpec}
import org.apache.spark.sql.SparkSession

object HelloWorld {
  def main(args: Array[String]): Unit = {
    var idSets: Set[Set[String]] = Set()
    val pattern = """\b([A-Za-z_]+)\b(?<!EQ|GE|LE)""".r // Match any word excluding EQ, GE, LE

    idSets += Set("hej", "dig")
    println(idSets)
    idSets += Set("med")
    println(idSets)

    idSets += pattern.findAllIn("o_col >= 24").toSet
    println(idSets)


    /*
    implicit val model: MPModel = MPModel(SolverLib.LpSolve)
    val x = MPFloatVar("x", 0, 2)

    var exprBuilder: Expression = null
    exprBuilder = 1 * (x - 4) * 7
    exprBuilder = exprBuilder * 2 - 4
    println(exprBuilder)
    var constr = new Constraint(exprBuilder, optimus.algebra.ConstraintRelation.GE, 8)
    println(constr.toString())

     */

    /*
    var setList: Seq[Set[String]] = Seq(Set("hej"))
    val s1 = "col > order"
    val pattern = """\w+""".r
    val res = pattern.findAllIn(s1).toSet
    println(res)
    println(setList :+ res)


    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val peopleDataset = Seq("001", "Bob", "002", "Joe").toDS

    */

/*
    implicit val model: MPModel = MPModel(SolverLib.LpSolve)
    val x = MPFloatVar("x", 100, 200)
    val y = MPFloatVar("y", 90, 170)

    var exprBuilder: Expression = null
    exprBuilder = 1 * (x - 4) * 7
    println(exprBuilder)
    exprBuilder = 1 * (x - 4) * 7 * 2 - 4 * y
    println(exprBuilder)

    val func: Expression = 1 + 2 + 3
    val const: Constraint = y >:= -x + 200
    // println(const.rhs.toString())
    var obj: Expression = x
    obj = obj + y
    maximize(obj)
    // maximize(-2 * x + 5 * y)
    add(const)
    start()


    println(s"objective: $objectiveValue")
    println(s"x = ${x.value} y = ${y.value}")
    release()

 */

    /*

    println(spark.sql("select age * 1 from person").queryExecution.optimizedPlan.numberedTreeString)
    spark.sql("")





     */

    /*
        object Problem extends ModelSpec(SolverLib.LpSolve) with App {

          val x = MPFloatVar(100, 200)
          val y = MPFloatVar(80, 170)
          val c: Constraint = new Constraint(y, optimus.algebra.ConstraintRelation.GE, 100)

          var exprBuilder: Expression = _
          exprBuilder = 1 * (x - 4) * 7
          exprBuilder = exprBuilder * 2 - 4

          maximize(-2 * x + 5 * y)
          add(y >:= -x + 200)

          start()

          release()
        }
     */


    // val data = Array(1, 2, 3, 4, 5)
    // val data2 = Seq(10, 20 , 30)
    // val distdata2 = sc.parallelize(data2)
    // val distData = sc.parallelize(data)
    // println(distData.reduce((a, b) => a + b))
    // println(distdata2.reduce((a, b) => a * b))
  }

}