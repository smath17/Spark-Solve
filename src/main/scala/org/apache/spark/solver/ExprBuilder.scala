package org.apache.spark.solver

import optimus.algebra.Expression
import optimus.optimization.MPModel

class ExprBuilder(indexMap: scala.collection.mutable.Map[String, Int], workingModel: MPModel) {
  val model: MPModel = workingModel

  def build(tokens: List[String]): Expression = {
    var tempBuilder: Expression = stringToExpr(tokens.head)
    var i = 1
    while (i <= tokens.length) {
      tokens(i) match {
        case operator if operator.matches("""+|\-|*""") =>
          tempBuilder = operatorNextToExpr(tokens(i), tokens(i + 1), tempBuilder)
          i += 2
        case _ => throw new Exception("Expected operator, got: " + tokens(i) + "... in " + tokens.toString())
      }
    }
    tempBuilder
  }

  // TODO: Fix case SUM cannot be wrapped in parentheses, therefor objective function may be calculated wrong
  private def stringToExpr(input: String): Expression = {
    var res: Expression = null
    var splitIndex = 0
    input match {
      case number if number.forall(char => char.isDigit) => res = number.toLong
      case name if name.matches("""[A-Za-z]""") => res = model.variable(indexMap(name)).get
      case sum if sum.startsWith("SUM") =>
        val aggSplitContent = sum.substring(3).split("""([+\-*/]|\w+)""")
        while (splitIndex <= aggSplitContent.length) {
          val split = aggSplitContent(splitIndex)
          split match {
            case op if op.matches("""[+-*]""") =>
              res = operatorNextToExpr(split, aggSplitContent(splitIndex + 1), res)
              splitIndex += 2
            case _ =>
              res = stringToExpr(split)
              splitIndex += 1
          }
        }
      case _ => throw new IllegalArgumentException("Invalid token found: " + input)
    }
    res
  }

  private def operatorNextToExpr(token: String, nextToken: String, exprBuilder: Expression): Expression = {
    var newExprBuilder = exprBuilder
    token match {
      case "+" => newExprBuilder = exprBuilder + stringToExpr(nextToken)
      case "-" => newExprBuilder = exprBuilder - stringToExpr(nextToken)
      case "*" => newExprBuilder = exprBuilder * stringToExpr(nextToken)
    }
    newExprBuilder
  }
}
