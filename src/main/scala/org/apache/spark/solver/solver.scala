package org.apache.spark.solver

import optimus.algebra.Constraint
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.lit

import scala.language.postfixOps

sealed trait Direction

case object Maximize extends Direction

case object Minimize extends Direction

case class PaProblem(uVariable: String, direction: Direction, objFunc: String, constraints: Seq[Constraint])


object solver {
  // class wrapper for Dataset
  implicit class DatasetSolve[T](ds: Dataset[T]) {

    // Extract variables and append. Result is a set of distinct variable sets
    def partition(constraints: Seq[Constraint]): Set[Set[String]] = {
      // Merge sets in a set, if they intersect
      def mergeIntersectingSets(sets: Set[Set[String]]): Set[Set[String]] = {
        if (sets.isEmpty) {
          Set.empty[Set[String]]
        } else {
          val cur = sets.head
          val merged = mergeIntersectingSets(sets.tail)
          val (hasCommon: Set[Set[String]], rest: Set[Set[String]]) = merged.partition(x => {
            (x & cur).nonEmpty
          })
          rest + (cur ++ hasCommon.flatten)
        }
      }

      var idSets: Set[Set[String]] = Set()
      val pattern = """\b([A-Za-z]+)\b(?<!EQ|GE|LE)""".r // Match any word excluding EQ, GE, LE

      // Append id sets from constraints
      for (constraint <- constraints) {
        idSets += pattern.findAllIn(constraint.toString()).toSet
      }
      // Create disjoint partitions
      mergeIntersectingSets(idSets)
      // Obj not considered in var sets
      // idSets += pattern.findAllIn(mainProblem.objFunc).toSet
    }

    def splitObjByParen(obj: String): Seq[String] = {
      val betweenParenPattern = """\(([^)]*)\)""".r // TODO: should match everything to ')'
      obj.split(')')
      betweenParenPattern.findAllIn(obj).toSeq
    }

    def buildAllSubs(partitions: Set[Set[String]], mainProblem: PaProblem): Seq[PaProblem] = {
      val res: Seq[PaProblem] = Seq()
      val objSplit = splitObjByParen(mainProblem.objFunc)
      for (partit <- partitions) {
        res :+ buildSub(partit, mainProblem, objSplit)
      }
      res
    }

    def buildSub(partition: Set[String], mainProblem: PaProblem, objSplit: Seq[String]): PaProblem = {
      val relatedConstr: Array[Constraint] = Array()
      var newObj = ""
      val direction = mainProblem.direction
      val unknownVars = mainProblem.uVariable.split(",\\s") // Assumes var1, var2
      val uVar = unknownVars.intersect(partition.toSeq).mkString //  TODO: Check integrity of PaProblem

      for (constraint <- mainProblem.constraints) {
        // If constraint contains at least 1 id from partition it is added to new problem
        if (partition.exists(idFromPar => constraint.toString().contains(idFromPar))) {
          relatedConstr :+ constraint
        }
      }

      for (split <- objSplit) {
        if (partition.exists(idFromPar => split.contains(idFromPar))) {
          newObj += split
        }
      }
      PaProblem(uVar, direction, newObj, relatedConstr)
    }

    // Problem must define names of new columns aka the variables
    // constraint and objective can be given by selects?
    def solve(newCols: Seq[String], mainProblem: PaProblem): Unit = {
      val data = this.ds
      data.collect()
      data.first()
      val rows = data.select("order")
      for (row <- rows) {

      }
      data.withColumn("result", lit(None))
      // 1. Partition input into sub-problems
      val partitions = partition(mainProblem.constraints)
      // 2. Build sub-problems from partitions
      val subProblems = buildAllSubs(partitions, mainProblem)
      // 3. Send to solver, collect result
      // 4. Return result
    }
  }

}