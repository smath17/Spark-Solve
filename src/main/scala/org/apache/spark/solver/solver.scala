package org.apache.spark.solver

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps

sealed trait Direction

case object Maximize extends Direction

case object Minimize extends Direction

// TODO: objFunc and constraints are unsafe as the strings are not checked to conform to expected format, or for spelling
// TODO: Problem occurs if newVarCols are substrings of each other
// TODO: make sure constraints are limited to >=, <=, =
case class PaProblem(newVarCols: Seq[UnknownVariableCol], direction: Direction, objFunc: String, constraints: Seq[String])


object solver {
  // class wrapper for Dataset
  implicit class DatasetSolve(ds: DataFrame) {

    /**
     * Extracts related variables from constraints and objective, then merges sets if they share elements. Result is a set of distinct variable sets
     *
     * @param objective   The objective function
     * @param constraints A collection of constraints
     * @return A set of disjoint ID sets
     */
    def partition(objective: String, constraints: Seq[String]): Set[Set[String]] = {
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

      def verifyInputNames(input: String): Unit = {
        val reservedNames = Seq("EQ", "GE", "LE")
        if (reservedNames.exists(input.contains)) {
          throw new IllegalArgumentException("Used reserved name in objective")
        }
      }

      var idSets: Set[Set[String]] = Set()
      // TODO: Don't exclude words
      val pattern = """\b([A-Za-z]+)\b(?<!EQ|GE|LE)""".r // Match any word excluding EQ, GE, LE

      // Check if reserved names are used
      constraints.foreach(constraint => verifyInputNames(constraint))
      verifyInputNames(objective)


      // Append id sets from constraints
      for (constraint <- constraints) {
        idSets += pattern.findAllIn(constraint).toSet
      }
      // Append id sets from objective, sets of length 1 are excluded. Loosely based on parentheses
      val objSplits = splitObjByParen(objective)
      //.filter(varSet => varSet.length > 1)
      objSplits.foreach(set => idSets += pattern.findAllIn(set).toSet.filter(idSet => idSet.length > 1))

      // Create and return disjoint partitions
      mergeIntersectingSets(idSets)
    }

    /**
     * Splits the objective string by spaces and parentheses
     * Splits are then concatenated so that matching parentheses are kept together
     *
     * Example:
     * y + (var1)
     * > [y], [+], [(], [var1], [)]
     * > [y], [+], [(var1)]
     *
     * @param obj the objective function of the PA problem. Expected to not be wrapped in parentheses
     * @return list of strings separated by spaces, parentheses are respected and kept together
     */
    def splitObjByParen(obj: String): Seq[String] = {
      obj.split(Array(' ', '('))
      // Regex pattern to split string into parentheses and
      // any that is not whitespace or parentheses (literals, variable names)
      val parenNotSpacePattern = """(\(|\)|[^\s()]+)""".r
      val splitObj = parenNotSpacePattern.findAllIn(obj).toBuffer
      // Merge splits based on matching parentheses
      for (i <- splitObj.indices) {
        val notEqParens = splitObj(i).count(_ == '(') == splitObj(i).count(_ == ')')
        if (notEqParens) {
          splitObj(i + 1) = splitObj(i) + splitObj(i + 1)
          splitObj.remove(i)
        }
      }
      splitObj
    }

    def buildAllSubs(partitions: Set[Set[String]], mainProblem: PaProblem): Seq[IntermediatePA] = {
      val res: Seq[IntermediatePA] = Seq()
      val objSplit = splitObjByParen(mainProblem.objFunc)
      for (partit <- partitions) {
        res :+ buildSub(partit, mainProblem, objSplit)
      }
      res
    }

    def buildSub(partition: Set[String], mainProblem: PaProblem, objSplit: Seq[String]): IntermediatePA = {
      val relatedConstr: Array[String] = Array()
      val newObj: ArrayBuffer[String] = ArrayBuffer()
      val direction = mainProblem.direction
      val unknownVarsInPartition = mainProblem.newVarCols.collect {
        case unknownVariable if partition.contains(unknownVariable.name) => unknownVariable
      }
      // Find related constraints
      for (constraint <- mainProblem.constraints) {
        // If constraint contains at least 1 id from partition it is added to new problem
        if (partition.exists(idFromPar => constraint.contains(idFromPar))) {
          relatedConstr :+ constraint
        }
      }

      // Find related parts of objective
      // Ex. 2 * SUM(Col1 * Col2)
      var i = 0
      while (i < objSplit.size) {
        val currentSplit = objSplit(i)
        currentSplit match {
          // If current is SUM and next contains related IDs => collect both as related objective
          case "SUM" | "COUNT" => if (partition.exists(idFromPar => objSplit(i + 1).contains(idFromPar))) {
            // If the objective already consist of elements, the operator is required
            if (newObj.length > 1) {
              newObj += (objSplit(i - 1), currentSplit, objSplit(i + 1))
            }
            else {
              newObj += (currentSplit, objSplit(i + 1))
            }
            // Increment twice to account for (i + 1)
            i += 2
          }
          // If current is in partition
          case split if partition.exists(idFromPar => split.contains(idFromPar)) =>
            newObj += split
            i += 1
          case _ =>
        }
      }
      new IntermediatePA(unknownVarsInPartition, direction, newObj, relatedConstr, this.ds)
    }

    // Problem must define names of new columns aka the variables
    // constraint and objective can be given by selects?
    def solve(mainProblem: PaProblem): Unit = {
      // Objective given as (Agg, Column, Operator, newCol: String)
      val data = this.ds


      data.col("order")
      data.collect()
      data.first()

      val rows = data.select("order")
      for (row <- rows) {
      }
      data.withColumn("result", lit(None))


      // 1. Partition input into sub-problems
      val partitions: Set[Set[String]] = partition(mainProblem.objFunc, mainProblem.constraints)
      // 2. Build sub-problems from partitions
      val subProblems: Seq[IntermediatePA] = buildAllSubs(partitions, mainProblem)
      // 3. Send to solver, collect result
      // 4. Return result
    }
  }
}