package de.hpi.ddm.jujo.exercise2

import org.apache.spark.sql._

import scala.collection.mutable.ListBuffer

object Exercise2 extends App {

  def defineSparkSession(numberOfCores: Int): SparkSession = {
    val sparkBuilder = SparkSession
      .builder()
      .appName("Exercise2")
      .master("local[" + numberOfCores + "]")
      .config("spark.sql.shuffle.partitions", (numberOfCores * 4).toString)
    sparkBuilder.getOrCreate()
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

    val dataFrames = Array(
      readNations(sparkSession, pathToTPCH),
      readRegions(sparkSession, pathToTPCH),
      readCustomers(sparkSession, pathToTPCH),
      readLineItems(sparkSession, pathToTPCH),
      readOrders(sparkSession, pathToTPCH),
      readParts(sparkSession, pathToTPCH),
      readSuppliers(sparkSession, pathToTPCH)
      )

    val values =
      dataFrames
      .flatMap(dataFrame => {
        var tuples = new ListBuffer[(String, String)]()
        val columnNames = dataFrame.columns

        dataFrame.collect().foreach(row => {
          for (columnIndex <- columnNames.indices) {
            tuples += ((columnNames.apply(columnIndex), row.get(columnIndex).toString))
          }
        })

        tuples.toArray
      } : Array[(String, String)])

    val columnValues =
      sparkSession.sparkContext.parallelize(values)
      .map(row => (row._1, row._2))
      .toDF()
      .dropDuplicates()
      .as[(String, String)]
      .groupByKey(_._1)
      .mapGroups{case(k, iter) => (k, iter.map(x => x._2).toSeq.sorted)}

    val result =
      columnValues.crossJoin(columnValues)
      .as[(String, Set[String], String, Set[String])]
      .filter(row => row._1 != row._3)
      .filter(row => {
        val unmatched = row._2--row._4
        unmatched.size < 1
      })
      .map(row => (row._1, row._3))
      .groupByKey(_._1)
      .mapGroups{case(k, iter) => (k, iter.map(_._2).toArray)}
      .as[(String, Array[String])]
      //.collect()

    result.foreach(row => {
      print(row._1 + " < ")

      row._2.foreach(value => print(value + ", "))
      println()
    })
  }
}
