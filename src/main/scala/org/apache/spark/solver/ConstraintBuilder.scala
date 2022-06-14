package org.apache.spark.solver

import optimus.algebra.{Constraint, Expression}
import optimus.optimization.MPModel
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

class ConstraintBuilder(modelVarMap: scala.collection.mutable.Map[String, Int], workingModel: MPModel, newVariableNames: Seq[String], data: DataFrame) {

  private def findOperator(tokens: String): String = {
    if (tokens.contains(">=")) {
      ">="
    }
    else if (tokens.contains("<=")) {
      "<="
    }
    else if (tokens.contains("=")) {
      "="
    }
    else {
      throw new IllegalArgumentException("Constraint missing constraint operator")
    }
  }

  def build(tokens: String): Array[Constraint] = {
    val resConstraints: scala.collection.mutable.ArrayBuffer[Constraint] = scala.collection.mutable.ArrayBuffer()
    // Global constraint
    val splitTokens = tokens.split(findOperator(tokens))
    if (splitTokens(0).contains("SUM") && splitTokens(1).contains("SUM")) {
      resConstraints += createGlobalConstraint(tokens)
    }
    else if (splitTokens(0).contains("SUM") && !splitTokens(1).contains("SUM")) {
      // Sum only lhs
      resConstraints += createGlobalConstraint(tokens, lhsAgg = true)
    }
    else if (!splitTokens(0).contains("SUM") && splitTokens(1).contains("SUM")) {
      // Sum only rhs
      resConstraints += createGlobalConstraint(tokens, lhsAgg = false)
    }
    // Per row constraint
    else {
      for (index <- data.collect().indices) {
        resConstraints += createConstraint(tokens, index)
      }
    }
    resConstraints.toArray
  }

  // Aggregate only 1 side of the constraint
  private def createGlobalConstraint(tokens: String, lhsAgg: Boolean): Unit = {
    val sideToAggregate: ArrayBuffer[Expression] = ArrayBuffer()
    var sideLiteral: Expression = null
    var side: Int = 0
    val expressions = splitStringIntoExpressions(tokens, findOperator(tokens), workingModel, 0)
    var lhsExpr: Expression = null
    var rhsExpr: Expression = null

    if (lhsAgg) {
      sideLiteral = expressions(1)
    }
    else {
      sideLiteral = expressions(0)
      side = 1
    }

    for (index <- data.collect().indices) {
      val expressions = splitStringIntoExpressions(tokens, findOperator(tokens), workingModel, index)
      sideToAggregate += expressions(side)
    }

    val aggExpr = sideToAggregate.toArray.reduce((expr1, expr2) => expr1 + expr2)
    if (lhsAgg) {
      lhsExpr = aggExpr
      rhsExpr = sideLiteral
    }

    findOperator(tokens) match {
      case ">=" => Constraint(lhsExpr, optimus.algebra.ConstraintRelation.GE, rhsExpr)
      case "<=" => Constraint(lhsExpr, optimus.algebra.ConstraintRelation.LE, rhsExpr)
      case "=" => Constraint(lhsExpr, optimus.algebra.ConstraintRelation.EQ, rhsExpr)
    }
  }

  private def createGlobalConstraint(tokens: String): Constraint = {
    val rhsExprs: ArrayBuffer[Expression] = ArrayBuffer()
    val lhsExprs: ArrayBuffer[Expression] = ArrayBuffer()
    for (index <- data.collect().indices) {
      val expressions = splitStringIntoExpressions(tokens, findOperator(tokens), workingModel, index)
      lhsExprs += expressions(0)
      rhsExprs += expressions(1)
    }
    val lhsExpr = lhsExprs.toArray.reduce((expr1, expr2) => expr1 + expr2)
    val rhsExpr = rhsExprs.toArray.reduce((expr1, expr2) => expr1 + expr2)

    findOperator(tokens) match {
      case ">=" => Constraint(lhsExpr, optimus.algebra.ConstraintRelation.GE, rhsExpr)
      case "<=" => Constraint(lhsExpr, optimus.algebra.ConstraintRelation.LE, rhsExpr)
      case "=" => Constraint(lhsExpr, optimus.algebra.ConstraintRelation.EQ, rhsExpr)
    }
  }

  private def createConstraint(tokens: String, index: Int): Constraint = {
    if (tokens.contains(">=")) {
      val Array(lhExpr, rhExpr) = splitStringIntoExpressions(tokens, ">=", workingModel, index)
      Constraint(lhExpr, optimus.algebra.ConstraintRelation.GE, rhExpr)
    }
    else if (tokens.contains("<=")) {
      val Array(lhExpr, rhExpr) = splitStringIntoExpressions(tokens, "<=", workingModel, index)
      Constraint(lhExpr, optimus.algebra.ConstraintRelation.LE, rhExpr)
    }
    else if (tokens.contains("=")) {
      val Array(lhExpr, rhExpr) = splitStringIntoExpressions(tokens, "=", workingModel, index)
      Constraint(lhExpr, optimus.algebra.ConstraintRelation.EQ, rhExpr)
    }
    else {
      throw new IllegalArgumentException("Constraint missing constraint operator")
    }
  }

  private def splitStringIntoExpressions(inputString: String, splitOperator: String, model: MPModel, index: Int): Array[Expression] = {
    val Array(lhs, rhs) = inputString.split(splitOperator)
    val opsAggsNames = """[+\-*/]|SUM(.*?)\)|\w+""".r // Split into operators, aggregators, names
    val lhsTokens = opsAggsNames.findAllIn(lhs).toList
    val rhsTokens = opsAggsNames.findAllIn(rhs).toList
    val exprBuilder: ExprBuilder = new ExprBuilder(modelVarMap, model, newVariableNames, data)
    val lhExpr: Expression = exprBuilder.build(lhsTokens, index)
    val rhExpr: Expression = exprBuilder.build(rhsTokens, index)
    Array(lhExpr, rhExpr)
  }
}
