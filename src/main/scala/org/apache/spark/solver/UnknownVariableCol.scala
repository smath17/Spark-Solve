package org.apache.spark.solver

import optimus.optimization.MPModel
import optimus.optimization.model.MPFloatVar

case class UnknownVariableCol(name: String, min: Double, max: Double) {
  // Overloaded constructor without interval
  def this(name: String) = this(name, Double.MinValue, Double.MaxValue)

  /**
   * Cast UnknownVariableCol to a ModelFloatVar and add to model.
   * Adds interval if present
   *
   * @param model     Current Optimus model.
   * @param extraName String to be appended on to column name, resulting in name_extraName.
   * @return Input model with a single variable added.
   */
  private def toModelVariable(model: MPModel, extraName: String): MPModel = {
    implicit val LPmodel: MPModel = model

    if (min == Double.MinValue && max == Double.MaxValue) {
      MPFloatVar(name + extraName)
    }
    else {
      MPFloatVar(name + "_" + extraName, min, max)
    }

    model
  }

  /**
   * Adds a model variable per row for the new column
   * Also modifies index map for later lookup
   *
   * @param model   Current Optimus model
   * @param numRows The number of rows in the input relation
   * @return Input model with variables for entire column
   */
  def addModelVariables(model: MPModel, numRows: Long, indexMap: scala.collection.mutable.Map[String, Int]): MPModel = {
    for (i <- 0 until numRows.toInt) {
      this.toModelVariable(model, i.toString)
      indexMap += ((name + "_" + i.toString) -> (indexMap.size + 1)) // TODO: Make sure map is updated, else return updated map
    }
    model
  }
}
