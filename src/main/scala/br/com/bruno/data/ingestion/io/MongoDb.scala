package br.com.bruno.data.ingestion.io

import br.com.bruno.data.ingestion.Utils
import br.com.bruno.data.ingestion.io.loader.IoLoader
import br.com.bruno.data.ingestion.model.{Step, Table}
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.mongodb.scala.{MongoClient, MongoDatabase, Observable}

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

class MongoDb(step: Step) extends IoLoader(step) {

  private val EXTRACT_DATABASE_NAME_REGEX = "(.*:)[0-9]*\\/(.*)([\\?]).*"

  def extractDatabaseName(url: String): String = {
    Utils.extractRegexGroup(EXTRACT_DATABASE_NAME_REGEX, url, 2)
  }

  override def discovery: ListBuffer[Table] = {
    val tables: ListBuffer[Table] = new ListBuffer[Table]
    // List All Collections from MongoDB
    val mongoClient: MongoClient = MongoClient(this.step.url)

    val databaseName: String = extractDatabaseName(this.step.url)
    this.step.sourceDatabase = databaseName

    val database: MongoDatabase = mongoClient.getDatabase(databaseName)

    val tableNames: Observable[String] = database.listCollectionNames()
    tableNames.subscribe((tableName: String) => tables += new Table(tableName))
    Await.result(tableNames.toFuture(), 10.minutes)

    tables
  }


  def loadTable(table: Table): DataFrame = {
    val spark = SparkSession.getDefaultSession.get
    val config: Map[String, String] = Map("uri" -> step.url, "collection" -> table.name)
    val readConfig = ReadConfig(config)
    MongoSpark.load(spark, readConfig)
  }
}
