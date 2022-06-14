package org.apache.spark.solver

import optimus.algebra.Expression
import optimus.optimization.MPModel
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

class ObjectiveBuilder(modelVarMap: scala.collection.mutable.Map[String, Int], workingModel: MPModel, newVariableNames: Seq[String], data: DataFrame) {
  val exprBuilder: ExprBuilder = new ExprBuilder(modelVarMap, workingModel, newVariableNames, data)

  def build(objSplits: Seq[String]): Expression = {
    val objectiveString = objSplits.mkString
    val opsAggsNames = """[+\-*/]|SUM(.*?)\)|\w+""".r // Split into operators, aggregators, names
    val objTokens = opsAggsNames.findAllIn(objectiveString).toList
    val partialObjectives: ArrayBuffer[Expression] = scala.collection.mutable.ArrayBuffer()
    for (row <- data.collect().indices) {
      partialObjectives += exprBuilder.build(objTokens, row)
    }
    partialObjectives.reduce((expr1, expr2) => expr1 + expr2)
  }
}
