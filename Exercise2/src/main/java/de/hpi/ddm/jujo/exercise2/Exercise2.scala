package de.hpi.ddm.jujo.exercise2

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable.ListBuffer

object Exercise2 extends App {

  var sparkSession: SparkSession

  def defineSparkSession(numberOfCores: Int): SparkSession = {
    val sparkBuilder = SparkSession
      .builder()
      .appName("Exercise2")
      .master("local[" + numberOfCores + "]")
      .config("spark.sql.shuffle.partitions", (numberOfCores * 2).toString)
    sparkSession = sparkBuilder.getOrCreate()
  }

  def readFromCSV(spark: SparkSession, pathToFile: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(pathToFile)
  }

  def readCustomers(spark: SparkSession, pathToTPCH: String): DataFrame = {
    readFromCSV(spark, pathToTPCH + "tpch_customer.csv")
  }

  def readLineItems(spark: SparkSession, pathToTPCH: String): DataFrame = {
    readFromCSV(spark, pathToTPCH + "tpch_lineitem.csv")
  }

  def readNations(spark: SparkSession, pathToTPCH: String): DataFrame = {
    readFromCSV(spark, pathToTPCH  + "tpch_nation.csv")
  }

  def readOrders(spark: SparkSession, pathToTPCH: String): DataFrame = {
    readFromCSV(spark, pathToTPCH  + "tpch_orders.csv")
  }

  def readParts(spark: SparkSession, pathToTPCH: String): DataFrame = {
    readFromCSV(spark, pathToTPCH  + "tpch_part.csv")
  }

  def readRegions(spark: SparkSession, pathToTPCH: String): DataFrame = {
    readFromCSV(spark, pathToTPCH + "tpch_region.csv")
  }

  def readSuppliers(spark: SparkSession, pathToTPCH: String): DataFrame = {
    readFromCSV(spark, pathToTPCH  + "tpch_supplier.csv")
  }

  def extractColumnsFromRow(columnNames: Array[String], row: Row): Array[(String, String)] = {
    var tuples = new ListBuffer[(String, String)]()
    for(i <- columnNames.indices) {
      tuples += ((columnNames.apply(i), row.get(i).toString))
    }
    tuples.toArray
  }

  override def main(args: Array[String]): Unit = {
    var numberOfCores = 4
    var pathToTPCH = "./TPCH"

    for(i <- args.indices) {
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
    import sparkSession.implicits._

    val nations = readNations(sparkSession, pathToTPCH)
    val regions = readRegions(sparkSession, pathToTPCH)

    val dataFrames = Array(nations, regions)

    val columNames = nations.columns

    dataFrames
      .flatMap(dataFrame => {
        val columnNames = dataFrame.columns
        dataFrame.flatMap(row => extractColumnsFromRow(columnNames, row))
      })
      .dropDuplicates()
      .rdd
      .groupByKey()
      .take(20)
      .foreach(println)
  }
}