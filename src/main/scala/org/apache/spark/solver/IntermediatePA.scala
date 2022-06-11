package org.apache.spark.solver

import optimus.algebra.{Constraint, Expression}
import optimus.optimization.enums.SolverLib
import optimus.optimization.model.MPFloatVar
import optimus.optimization.{MPModel, add}
import org.apache.spark.sql.DataFrame

class IntermediatePA(newVarCols: Seq[String], direction: Direction, objFunc: Seq[String], constraints: Seq[String], inputRelation: DataFrame) {
  def toLPModel: MPModel = {
    implicit var model: MPModel = MPModel(SolverLib.oJSolver)
    model = createModelVars(model)
    model = createModelConstraints(model)
    model = createModelObjective(model)

    model
  }

  private def createModelVars(model: MPModel): MPModel = {
    implicit val lpModel: MPModel = model

    // TODO: For this to work properly, simple constrains should be added here, i.e. var1 > 0
    // Create variable for new columns, meaning 1 per row in the input
    for (col <- newVarCols) {
      for (rowCount <- 0 until inputRelation.count().toInt) {
        MPFloatVar(col + "_" + rowCount.toString)
      }
    }
    lpModel
  }

  private def createModelConstraints(model: MPModel): MPModel = {

    /*
    def stringToConstraint(model: MPModel, constrString: String, op: String): Unit = {
      implicit val constraintModel: MPModel = model
      MPFloatVar("x")  // Ignore IDE error. IntelliJ has troubles keeping up with many implicits


      val Array(lhs, rhs) = constrString.split(op)
      val lhsTokens = """[+|\-*/
    //   ]|\w+""".r.findAllIn(lhs).toList
    // TODO: lhs symbol rhs
    //  get values inputRelation.col("")(i) and variables lpModel.variable(col + "_" + i.toString).get
    // }


    implicit val lpModel: MPModel = model

    for (constr <- constraints) {
      constr match {
        case x if x.contains(">=") => {
          val Array(lhs, rhs) = constr.split(">=")
          val lhsTokens = """[+|\-*/]|\w+""".r.findAllIn(lhs).toList
          var lhExprBuilder: Expression = lhsTokens.head
          // TODO: For each token in lhs, append to builder with appropriate operator
          //  Special case for index 0, this should not be appended but set with =

          // TODO: match for operator
          for (token <- lhsTokens) {
            token match {
              case sum if sum.contains("SUM") => ???
              case count if count.contains("COUNT") => ???
              case name if name.matches("""[A-Za-z]""") => _
              case _ => lhExprBuilder = lhExprBuilder + token.toDouble

            }
          }

          val lhsExpr = 1 + 2
          val dddd: Expression = lhsExpr
          // val ddddc: Expression = ddddc + lhsExpr
          val c: Constraint = Constraint(inputRelation.select("").collect()(0).getInt(0), optimus.algebra.ConstraintRelation.GE, 2)
          inputRelation.collect()(0)
          inputRelation.rdd.take(0)

          // TODO: lhs symbol rhs
          //  get values inputRelation.col("")(i) and variables lpModel.variable(col + "_" + i.toString).get

        }
        case x if x.contains("<=") => ???
        case x if x.contains("=") => ???
      }
      for (rowCount <- 0 until inputRelation.count().toInt) {
        add(lpModel.variable(rowCount.toInt).get >:= 0)
      }
    }

    model
  }

  private def createModelObjective(model: MPModel): MPModel = ???

  private def valueFromDF(col: String, rowNum: Int): Unit = {
    inputRelation.select(col).collect()(rowNum).getInt(0)
  }
}