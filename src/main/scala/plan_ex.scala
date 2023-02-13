import org.apache.spark.sql.{Dataset, SparkSession}

// Business object
/*
case class Persona(id: String, name: String, age: Int)

case class objective(direction: String, function: String)
case class PaProblem(uVariable: String, objFunc: objective, constraints: Seq[String])

object solver {
  /**
   * Partitions a PaProblem into two sub-problems
   *
   * @param mainProblem the original problem, which is to be partitioned
   * @return
   */
  def partition(mainProblem: PaProblem): (PaProblem, PaProblem) = {
    // Extract variables and append to set
    var varSet = Seq[String]()
    val pattern = "([A-Za-z_]*)".r
    varSet ++= (pattern findAllIn(mainProblem.objFunc.function)).toSeq
    varSet ++= mainProblem.constraints.map(s => pattern findAllIn(s))

    // Create 2 disjoint partitions

    (mainProblem, mainProblem)
  }
  def solve[T](input: Dataset[T], mainProblem: PaProblem): Unit = {

  }
}

// The dataset to query
object plan_ex {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").getOrCreate()
    import spark.implicits._

    // spark.conf.set("spark.sql.shuffle.partitions", "2")

    val peopleDataset = Seq(
      Persona("001", "Bob", 28),
      Persona("003", "Bob", 50),
      Persona("002", "Joe", 34)).toDS

    // The query to execute
    val query = peopleDataset
      .select("*").select("*")
      .groupBy("name")
      .count()
      .as("total")

    val query2 = peopleDataset.select("*").groupBy("name").sum().as("total")
    val query3 = peopleDataset.where("id > 0")
    val query4 = spark.sql("SELECT * FROM peopleDataset WHERE id > 0")
    // Get Catalyst optimization plan
    query4.show()
    // query.select("*").show()
    query4.explain(extended = true)
    spark.stop()
  }
}

 */