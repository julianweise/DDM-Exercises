package de.hpi.ddm.jujo.exercise2

import java.io.File

import org.apache.spark.sql._

import scala.collection.mutable.ListBuffer
import scala.io.Source

object Exercise2 extends App {

  def defineSparkSession(numberOfCores: Int): SparkSession = {
    val sparkBuilder = SparkSession
      .builder()
      .appName("Exercise2")
      .master("local[" + numberOfCores + "]")
      .config("spark.sql.shuffle.partitions", (numberOfCores * 2).toString)
    sparkBuilder.getOrCreate()
  }

  def readTuplesFromCSV(pathToFile: String): Set[(String, String)] = {
    val header = new ListBuffer[String]
    val cells = scala.collection.mutable.Set[(String, String)]()
    var firstLine = true
    for (line <- Source.fromFile(pathToFile).getLines) {
      if (firstLine) {
        header.appendAll(line.split(";"))
        header.map(headerCell => headerCell.replace(" ", "").replace("\"", ""))
        firstLine = false
      } else {
        for((value, index) <- line.split(";").zipWithIndex) {
          if (index < header.length) {
            cells.+=((value.replace(" ", "").replace("\"", ""), header.apply(index)))
          }
        }
      }
    }
    cells.toSet
  }

  def readAllFiles(pathToFolder: String): List[(String, String)] = {
    val file = new File(pathToFolder)
    val allCells = new ListBuffer[(String, String)]
    file.listFiles.filter(_.isFile)
      .filter(_.getName.endsWith(".csv"))
      .map(_.getPath).toList
      .foreach(file => allCells.appendAll(readTuplesFromCSV(file)))
    allCells.toList
  }

  def buildInclusionList(attributeSet: List[String]): List[(String, Set[String])] = {
    val inclusionList = new ListBuffer[(String, Set[String])]
    attributeSet.foreach(attribute => {
      inclusionList.append((attribute, attributeSet.toSet diff Set(attribute)))
    })
    inclusionList.toList
  }

  def intersectInclusionLists(inclusionLists: Iterable[(String, Set[String])]): Set[String] = {
    inclusionLists.map(tuple => tuple._2).reduce((accumulator, value) => accumulator intersect value)
  }

  def printResultLine(key: String, references: Set[String]): Unit = {
    val resultLineBuilder = StringBuilder.newBuilder
    resultLineBuilder.append(key + " < ")
    var prefix = ""
    for (elem <- references) {
      resultLineBuilder.append(prefix)
      resultLineBuilder.append(elem)
      prefix = ", "
    }
    println(resultLineBuilder.toString())
  }

  override def main(args: Array[String]): Unit = {
    var numberOfCores = 4
    var pathToTPCH = "./TPCH"

    for (i <- args.indices) {
      if (args.apply(i) == "--path") {
        pathToTPCH = args.apply(i + 1)
      } else if (args.apply(i) == "--cores") {
        numberOfCores = args.apply(i + 1).toInt
      }
    }

    if (pathToTPCH.takeRight(1) != "/") {
      pathToTPCH += "/"
    }

    val sparkSession = defineSparkSession(numberOfCores)

    val allCells = readAllFiles(pathToTPCH)


    sparkSession.sparkContext.parallelize(allCells)
      .repartition(numberOfCores)
      .groupBy(cell => cell._1)
      .map(keyListTuple => keyListTuple._2.map(groupedTuple => groupedTuple._2))
      .flatMap(attributeSet => buildInclusionList(attributeSet.toList))
      .groupBy(inclusionList => inclusionList._1)
      .map(inclusionListTuple => (inclusionListTuple._1, intersectInclusionLists(inclusionListTuple._2)))
      .filter(inclusionListTuple => inclusionListTuple._2.nonEmpty)
      .sortBy(inclusionListTuple => inclusionListTuple._1)
      .foreach(inclusionListTuple => printResultLine(inclusionListTuple._1, inclusionListTuple._2))
  }
}
