package org.apache.spark.solver

import optimus.algebra.{Constraint, Expression}
import optimus.optimization.enums.SolverLib
import optimus.optimization.{MPModel, add}
import org.apache.spark.sql.DataFrame

class IntermediatePA(newVarCols: Seq[UnknownVariableCol], direction: Direction, objFunc: Seq[String], constraints: Seq[String], inputRelation: DataFrame) {
  // Required to look up variables later
  val modelVarIndexMap: scala.collection.mutable.Map[String, Int] = scala.collection.mutable.Map()

  def toLPModel: MPModel = {
    implicit var model: MPModel = MPModel(SolverLib.oJSolver)
    model = createModelVars(model)
    model = createModelConstraints(model)
    model = createModelObjective(model)

    model
  }

  private def createModelVars(model: MPModel): MPModel = {
    var lpModel: MPModel = model
    val rowCount: Long = inputRelation.count()

    for (col <- newVarCols) {
      lpModel = col.addModelVariables(lpModel, rowCount, modelVarIndexMap)
    }

    lpModel
  }

  /**
   * Splits input string into expressions of left-hand and right-hand side of the split string
   *
   * @param inputString   String to be split
   * @param splitOperator String used to split input
   * @param model         Current MPModel, used to lookup premade variables
   * @return A pair (lhs, rhs) of type Array[Expression]
   */
  private def splitStringIntoExpressions(inputString: String, splitOperator: String, model: MPModel): Array[Expression] = {
    val Array(lhs, rhs) = inputString.split(splitOperator)
    val opsAggsNames = """[+\-*/]|SUM(.*?)\)|\w+""".r // Split into operators, aggregators, names
    val lhsTokens = opsAggsNames.findAllIn(lhs).toList
    val rhsTokens = opsAggsNames.findAllIn(rhs).toList
    val lhExpr: Expression = new ExprBuilder(modelVarIndexMap, model).build(lhsTokens)
    val rhExpr: Expression = new ExprBuilder(modelVarIndexMap, model).build(rhsTokens)
    Array(lhExpr, rhExpr)
  }

  private def createModelConstraints(model: MPModel): MPModel = {
    implicit val lpModel: MPModel = model

    for (constraint <- constraints) {
      constraint match {
        case ge if ge.contains(">=") =>
          val Array(lhExpr, rhExpr) = splitStringIntoExpressions(ge, ">=", lpModel)
          add(new Constraint(lhExpr, optimus.algebra.ConstraintRelation.GE, rhExpr))
        case le if le.contains("<=") =>
          val Array(lhExpr, rhExpr) = splitStringIntoExpressions(le, "<=", lpModel)
          add(new Constraint(lhExpr, optimus.algebra.ConstraintRelation.LE, rhExpr))
        case eq if eq.contains("=") =>
          val Array(lhExpr, rhExpr) = splitStringIntoExpressions(eq, "=", lpModel)
          add(new Constraint(lhExpr, optimus.algebra.ConstraintRelation.EQ, rhExpr))
      }
    }
    lpModel
  }


  private def createModelObjective(model: MPModel): MPModel = ???

  private def valueFromDF(col: String, rowNum: Int): Unit = {
    inputRelation.select(col).collect()(rowNum).getInt(0)
  }
}