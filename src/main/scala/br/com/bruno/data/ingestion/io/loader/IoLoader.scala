package br.com.bruno.data.ingestion.io.loader

import java.text.SimpleDateFormat
import java.util.concurrent.Executors

import br.com.bruno.data.ingestion.google.bigquery.{CreateDisposition, WriteDisposition}
import br.com.bruno.data.ingestion.model.{Step, Table}
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


abstract class IoLoader(var step: Step) extends Serializable {

  val logger = LoggerFactory.getLogger(this.getClass)

  def discovery: mutable.ListBuffer[Table]

  def loadTable(table: Table): DataFrame

  def execute() = {
    val dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    var tables: ListBuffer[Table] = null
    // Discovery All Tables/ Collections from Source DataBase
    try {
      tables = discovery
    } catch {
      case ex: Exception =>
        logger.error(s"Error on discovery process source type: ${step.sourceType} url: ${step.url} source database: ${step.sourceDatabase}", ex)
        System.exit(-1)
    }

    //  Thread Pool
    implicit val ec = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(step.threads))


    val tasks = for (i <- 0 to (tables.size - 1)) yield Future {
      println("Start Thread[" + i + "] Table [" + tables(i).name + "]")
      // Gets one SQL Context from the pool
      try {
        // Process each table in a different Thread, the quantity of threads are configured in configuration file.
        val sourceDataFrame: DataFrame = loadTable(tables(i))
        // Save result
        createReplaceBigQueryTable(sourceDataFrame, tables(i), step);
      } catch {
        case ex: Throwable =>
          println("Error processing thread " + i + " table " + tables(i).name)
          ex.printStackTrace(System.out)
      } finally {
        // Release the SQLContext
      }
      println("End Thread[" + i + "] Table [" + tables(i).name + "]")
    }


    val aggregated = Future.sequence(tasks)
    val date = new java.util.Date().getTime()
    println("[" + date + "][" + dateFormatter.format(new java.util.Date) + "] Start Waiting for Table Processing ")
    val oneToNSum = Await.result(aggregated, this.step.executionTimeoutInMinutes.minutes).size
    logger.debug("This is the end! My only friend... the end! % Threads left", oneToNSum)
    System.exit(0)

  }


  private def createReplaceBigQueryTable(sourceDataFrame: DataFrame, table: Table, step: Step) = {
    sourceDataFrame.saveAsBigQueryTable(step.targetDatabase + "." + table.name, WriteDisposition.WRITE_TRUNCATE, CreateDisposition.CREATE_IF_NEEDED, tmpWriteOptions = Map("recordNamespace" -> "br.com.justto"))
  }


  private def createAppendBigQueryTable(sourceDataFrame: DataFrame, table: Table, step: Step) = {
    sourceDataFrame.saveAsBigQueryTable(step.targetDatabase + "." + table.name, WriteDisposition.WRITE_APPEND, CreateDisposition.CREATE_IF_NEEDED, tmpWriteOptions = Map("recordNamespace" -> "br.com.justto"))
  }

}
