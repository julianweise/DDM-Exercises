package de.hpi.ddm.jujo.exercise2

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class Main extends App {

  def defineSparkSession(numberOfCores: Int): SparkSession = {
    val sparkBuilder = SparkSession
      .builder()
      .appName("Exercise2")
      .master("local[" + numberOfCores + "]")
      .config("spark.sql.shuffle.partitions", (numberOfCores * 2).toString)
    val spark = sparkBuilder.getOrCreate()
    spark
  }

  def readFromCSV(spark: SparkSession, pathToFile: String): DataFrame = {
    spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("delimiter", ";")
      .csv(pathToFile)
  }

  def readCustomers(spark: SparkSession, pathToTPCH: String): Dataset[(Long, String, String, Int, String, Double, String, String)] = {
    import spark.implicits._
    readFromCSV(spark, pathToTPCH + "tpch_customer.csv").as[(Long, String, String, Int, String, Double, String, String)]
  }

  def readLineItems(spark: SparkSession, pathToTPCH: String): Dataset[(Long, Long, Long, Long, Double, Double, Double, Double, Char, Char, String, String, String, String, String, String)] = {
    import spark.implicits._
    readFromCSV(spark, pathToTPCH + "tpch_lineitem.csv").as[(Long, Long, Long, Long, Double, Double, Double, Double, Char, Char, String, String, String, String, String, String)]
  }

  def readNations(spark: SparkSession, pathToTPCH: String): Dataset[(Long, String, Long, String)] = {
    import spark.implicits._
    readFromCSV(spark, pathToTPCH  + "tpch_nation.csv").as[(Long, String, Long, String)]
  }

  def readOrders(spark: SparkSession, pathToTPCH: String): Dataset[(Long, Long, Char, Double, String, String, String, Long, String)] = {
    import spark.implicits._
    readFromCSV(spark, pathToTPCH  + "tpch_orders.csv").as[(Long, Long, Char, Double, String, String, String, Long, String)]
  }

  def readParts(spark: SparkSession, pathToTPCH: String): Dataset[(Long, String, String, String, String, Long, String, Double, String)] = {
    import spark.implicits._
    readFromCSV(spark, pathToTPCH  + "tpch_part.csv").as[(Long, String, String, String, String, Long, String, Double, String)]
  }

  def readRegions(spark: SparkSession, pathToTPCH: String): Dataset[(Long, String, String)] = {
    import spark.implicits._
    readFromCSV(spark, pathToTPCH + "tpch_region.csv").as[(Long, String, String)]
  }

  def readSuppliers(spark: SparkSession, pathToTPCH: String): Dataset[(Long, String, String, Long, String, Double, String)] = {
    import spark.implicits._
    readFromCSV(spark, pathToTPCH  + "tpch_supplier.csv").as[(Long, String, String, Long, String, Double, String)]
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

    val nations = readNations(sparkSession, pathToTPCH)
    val regions = readRegions(sparkSession, pathToTPCH)

  }
}
