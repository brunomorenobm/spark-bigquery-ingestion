package br.com.bruno.data.ingestion

import br.com.bruno.data.ingestion.io.loader.IoLoaderFactory
import br.com.bruno.data.ingestion.model.Step
import org.apache.spark.sql.SparkSession

object Starter extends App {

  if (args.size < 6) {
    throw new Exception("Error initializing job, please use <type:jdbc> <url> <user> <password> <target-project>:<target-database> <execution-timeout-in-minutes> <?thread-count> <properties[key:value;key:value]>?")
  }

  // Read Configuration From Args
  val step = new Step
  step.sourceType = args(0)
  step.url = args(1)
  step.user = args(2)
  step.password = args(3)
  step.targetDatabase = args(4)
  step.executionTimeoutInMinutes = args(5).toInt
  step.threads =
    if (args.length > 6)
      args(6).toInt
    else
      20


  // Start Spark Context
  val spark = SparkSession.builder
    .appName("spark-bigquery-ingestion")
    .getOrCreate()

  // Execute The Load
  try {
    IoLoaderFactory.createIoLoader(step).execute()
  } catch {
    case ex: Exception =>
      println("Error execution Load from Step")
      ex.printStackTrace(System.err)
      System.exit(1)
  }

}
