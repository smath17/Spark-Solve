package org.apache.spark.solver

import optimus.algebra.Constraint
import optimus.optimization.enums.SolverLib
import optimus.optimization.{MPModel, add, maximize, minimize}
import org.apache.spark.sql.DataFrame

class IntermediatePA(val newVarCols: Seq[UnknownVariableCol], direction: Direction, objFunc: Seq[String], constraints: Seq[String], inputRelation: DataFrame) {
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

  /*
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
      val opsAggsNames = """[+\-*]|SUM(.*?)\)|\w+""".r // Split into operators, aggregators, names
      val lhsTokens = opsAggsNames.findAllIn(lhs).toList
      val rhsTokens = opsAggsNames.findAllIn(rhs).toList
      val exprBuilder: ExprBuilder = new ExprBuilder(modelVarIndexMap, model, newVarCols.map(col => col.name), inputRelation)
      val lhExpr: Expression = exprBuilder.build(lhsTokens)
      val rhExpr: Expression = exprBuilder.build(rhsTokens)
      Array(lhExpr, rhExpr)
    }
    */

  private def createModelConstraints(model: MPModel): MPModel = {
    implicit val lpModel: MPModel = model

    val modelConstraints: Array[Constraint] = Array()
    val constraintBuilder: ConstraintBuilder = new ConstraintBuilder(modelVarIndexMap, model, newVarCols.map(col => col.name), inputRelation)
    for (constraint <- constraints) {
      modelConstraints ++ constraintBuilder.build(constraint)
    }
    for (modelConstraint <- modelConstraints) {
      add(modelConstraint)
    }
    lpModel
  }

  private def createModelObjective(model: MPModel): MPModel = {
    implicit val lpModel: MPModel = model

    val objectiveBuilder = new ObjectiveBuilder(modelVarIndexMap, lpModel, newVarCols.map(col => col.name), inputRelation)
    val objectiveExpression = objectiveBuilder.build(objFunc)
    direction match {
      case Maximize => maximize(objectiveExpression)
      case Minimize => minimize(objectiveExpression)
    }
    lpModel
  }
}