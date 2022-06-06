package Spark

import org.apache.spark.sql.Dataset

import lpsolve._

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

sealed trait Direction

case object Maximize extends Direction

case object Minimize extends Direction

case class PaProblem(uVariable: String, direction: Direction, objFunc: String, constraints: Seq[String])


object solver {
  // class wrapper for Dataset
  implicit class DatasetSolve[T](ds: Dataset[T]) {

    // TODO: Sub-function inside partition
    // Merge sets in a set, if they intersect
    private def mergeSets(sets: Set[Set[String]]): Set[Set[String]] = {
      if (sets.isEmpty) {
        Set.empty[Set[String]]
      } else {
        val cur = sets.head
        val merged = mergeSets(sets.tail)
        val (hasCommon: Set[Set[String]], rest: Set[Set[String]]) = merged.partition(x => {
          (x & cur).nonEmpty
        })
        rest + (cur ++ hasCommon.flatten)
      }
    }

    // Extract variables and append. Result is a set of distinct variable sets
    def partition(constraints: Seq[String]): Set[Set[String]] = {
      var idSets: Set[Set[String]] = Set[Set[String]]
      val pattern = """\w+""".r

      // Append id sets from constraints
      for (constraint <- constraints) {
        idSets += pattern.findAllIn(constraint).toSet
      }
      // Create disjoint partitions
      mergeSets(idSets)
      // Obj not considered in var sets
      // idSets += pattern.findAllIn(mainProblem.objFunc).toSet
    }

    def buildSubs(partitions: Set[Set[String]], mainProblem: PaProblem): Seq[PaProblem] = {
      val res: Seq[PaProblem] = Seq()
      val constraints = Array()
      // Gather constraints
      for (partit <- partitions) {
        for (constraint <- mainProblem.constraints) {
          // If constraint contains at least 1 id from partition it is added to new problem
          if (partit.exists(idFromSet => constraint.contains(idFromSet))) {

          }
        }
      }

    }

    def buildSub(partition: Set[String], mainProblem: PaProblem): PaProblem = {
      val res = ???

      for (constraint <- mainProblem.constraints) {
        // If constraint contains at least 1 id from partition it is added to new problem
        if (partition.exists(idFromSet => constraint.contains(idFromSet))) {

        }
      }

      res
    }

    def solve(mainProblem: PaProblem): Unit = {
      val data = this.ds
      // 1. Partition input into sub-problems
      val partitions = partition(mainProblem.constraints)
      // 2. Build sub-problems from partitions
      // 3. Send to solver, collect result
      // 4. Return result
    }
  }

}