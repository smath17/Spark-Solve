package org.apache.spark.solver

import optimus.algebra.Expression
import optimus.optimization.MPModel
import org.apache.spark.sql.DataFrame

class ExprBuilder(indexMap: scala.collection.mutable.Map[String, Int], workingModel: MPModel, newVariableNames: Seq[String], data: DataFrame) {
  val model: MPModel = workingModel

  def build(tokens: List[String], rowIndex: Int): Expression = {
    var tempBuilder: Expression = stringToExpr(tokens.head, rowIndex)
    var i = 1
    while (i <= tokens.length) {
      tokens(i) match {
        case operator if operator.matches("""+|\-|*""") =>
          tempBuilder = operatorNextToExpr(tokens(i), tokens(i + 1), tempBuilder, rowIndex)
          i += 2
        case _ => throw new Exception("Expected operator, got: " + tokens(i) + "... in " + tokens.toString())
      }
    }
    tempBuilder
  }

  // TODO: Fix case SUM cannot be wrapped in parentheses, therefor objective function may be calculated wrong
  private def stringToExpr(input: String, rowIndex: Int): Expression = {
    var res: Expression = null
    var splitIndex = 0
    val tableVariableNames: Array[String] = data.columns
    input match {
      case number if number.forall(char => char.isDigit) => res = number.toLong
      // TODO: Check if name if unknown variable then do this, else retrieve value from df
      case name if name.matches("""[A-Za-z]""") =>
        name match {
          case unknownVar if newVariableNames.contains(unknownVar) => res = model.variable(indexMap(name + "_" + rowIndex)).get
          case tableVar if tableVariableNames.contains(tableVar) => res = valueFromDF(tableVar, rowIndex)
        }
      // TODO: unwrap for all rows in data
      case sum if sum.startsWith("SUM") =>
        val aggSplitContent = sum.substring(3).split("""([+\-*/]|\w+)""")
        while (splitIndex <= aggSplitContent.length) {
          val split = aggSplitContent(splitIndex)
          split match {
            case op if op.matches("""[+-*]""") =>
              res = operatorNextToExpr(split, aggSplitContent(splitIndex + 1), res, rowIndex)
              splitIndex += 2
            case _ =>
              res = stringToExpr(split, rowIndex)
              splitIndex += 1
          }
        }
      case _ => throw new IllegalArgumentException("Invalid token found: " + input)
    }
    res
  }

  private def operatorNextToExpr(token: String, nextToken: String, exprBuilder: Expression, rowIndex: Int): Expression = {
    var newExprBuilder = exprBuilder
    token match {
      case "+" => newExprBuilder = exprBuilder + stringToExpr(nextToken, rowIndex)
      case "-" => newExprBuilder = exprBuilder - stringToExpr(nextToken, rowIndex)
      case "*" => newExprBuilder = exprBuilder * stringToExpr(nextToken, rowIndex)
    }
    newExprBuilder
  }

  // Very unsafe, mostly works for integers
  private def valueFromDF(col: String, rowNum: Int): Int = {
    data.select(col).collect()(rowNum).getInt(0)
  }
}
