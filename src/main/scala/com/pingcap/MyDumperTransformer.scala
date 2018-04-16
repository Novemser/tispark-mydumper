package com.pingcap

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.commons.cli.{DefaultParser, HelpFormatter}
import org.apache.spark.sql.{SparkSession, TiContext}


object MyDumperTransformer extends LazyLogging {
  val MB = 1024 * 1024

  val spark: SparkSession = SparkSession
    .builder()
    .appName("MyDumperTransformer")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val ti = new TiContext(spark)
    val transformer = new Transformer(spark)
    val parser = new DefaultParser
    val cmd = parser.parse(transformer.getOptions, args)
    if (cmd.hasOption("help") || args.length <= 0) {
      val formatter = new HelpFormatter
      formatter.printHelp("java -jar tidb-tpch-dataloader-1.0-SNAPSHOT-jar-with-dependencies.jar [option]<arg>\nTransformer - TiSpark to mydumper SQL files.\n", transformer.getOptions)
      return
    }
    if (cmd.hasOption("chunkFileSize")) transformer.MAX_BYTES_PER_FILE = MB * cmd.getOptionValue("chunkFileSize").toLong
    if (cmd.hasOption("outputDir")) {
      val dir = cmd.getOptionValue("outputDir")
      transformer.OUTPUT_DIR = if (dir.endsWith("/")) dir
      else dir + "/"
      println("Output dir:\t" + transformer.OUTPUT_DIR)
    }
    else {
      println("You must specify -outputDir.")
      return
    }
    if (cmd.hasOption("rowCount")) transformer.MAX_ROWS_COUNT = cmd.getOptionValue("rowCount").toInt
    if (cmd.hasOption("dbName")) transformer.dbName = cmd.getOptionValue("dbName")
    ti.tidbMapDatabase(transformer.dbName)
    var readers = 2
    var writers = 2
    if (cmd.hasOption("readers")) readers = cmd.getOptionValue("readers").toInt
    if (cmd.hasOption("writers")) writers = cmd.getOptionValue("writers").toInt

    var tables: Array[String] = null
    if (cmd.hasOption("tables")) {
      val str = cmd.getOptionValue("tables")
      // [t1,t2,t3,t4]
      try {
        tables = str.split(",").map(_.trim).filter(!_.isEmpty)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          println("Invalid `tables` option, please refer to help.")
          return
      }
    } else {
      println("Please specify at least one table.")
      return
    }

    transformer.setTables(tables)
    transformer.run(1, 1)
  }
}