package br.com.bruno.data.ingestion.io

import br.com.bruno.data.ingestion.Utils
import br.com.bruno.data.ingestion.io.loader.IoLoader
import br.com.bruno.data.ingestion.model.{Step, Table}
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.StorageOptions
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

class GoogleStorage(step: Step) extends IoLoader(step) {

  private val EXTRACT_BUCKET_NAME_REGEX = "([^\\/]*)(\\/)(.*)"
  private val EXTRACT_FILE_EXTENSION_REGEX = "(.*)\\.(avro|parquet|xml|json|xls|xlsx|csv|txt)(.snappy|.gz)?(.part.*)?$"
  private val EXTRACT_FOLDER_NAME_REGEX = ".*\\/((.*)\\/)$"

  // Extract Information from String URL
  parseUrl(step.url)


  def parseUrl(url: String): Unit = {
    this.step.sourceDatabase = Utils.extractRegexGroup(EXTRACT_BUCKET_NAME_REGEX, url, 1)
    this.step.url = Utils.extractRegexGroup(EXTRACT_BUCKET_NAME_REGEX, url, 3)
  }

  override def discovery: ListBuffer[Table] = {

    val tables: ListBuffer[Table] = new ListBuffer[Table]
    // Start Google service Account
    val storage = StorageOptions.getDefaultInstance.getService

    // List all files from path
    val files = storage.list(step.sourceDatabase, BlobListOption.currentDirectory(), BlobListOption.prefix(step.url))

    //Get all child data folders
    val dataFolders = files.iterateAll().asScala.filter(p => p.getName != this.step.url)

    for (dataFolder <- dataFolders) {
      val table: Table = new Table(Utils.extractRegexGroup(EXTRACT_FOLDER_NAME_REGEX, dataFolder.getName, 2))
      // List all child files
      val dataFiles = storage.list(step.sourceDatabase, BlobListOption.currentDirectory(), BlobListOption.prefix(dataFolder.getName)).iterateAll()

      // Read file extensions to infer the Data Type, first map All Extensions with 1, and reduce by Key
      val extension = dataFiles.asScala
        // Map all extensions
        .map(file => (Utils.extractRegexGroup(EXTRACT_FILE_EXTENSION_REGEX, file.getName.toLowerCase, 2)))
        // Group by key
        .groupBy(identity)
        // Sum extensions
        .mapValues(_.size)
        // Rank and get the biggest one, then the Key _1
        .maxBy(extension => extension._2) _1

      // Set the extension of the max occurence
      table.objectType = extension

      //inferType
      if (table.objectType != null) {
        tables += table
      } else {
        // TODO: Add error threatment here
        println(s"Folder: $dataFolder do not have supported data files ")
      }
    }
    tables
  }


  def loadTable(table: Table): DataFrame = {
    val spark = SparkSession.getDefaultSession.get

    val strPath = s"gs://${step.sourceDatabase}/${step.url}/${table.name}"
    println(s"Reading folder: $strPath parsing format: ${table.objectType} for table: ${table.name}")
    val sourceDataFrame =
      table.objectType match {
        case "xml" =>
          spark.read.format("com.databricks.spark.xml")
            // The row tag MUST have the same nome of the Folder that holds the data files
            .option("rowTag", table.name)
            // Sets null to wrong data types and sets null for malformed rows and put the content into column "_corrupt_record"
            .option("mode", "PERMISSIVE")
            .option("path", strPath)
            .load()
        case "parquet" =>
          spark.read.parquet(strPath)
        case "json" =>
          spark.read.json(strPath)
        case "csv" =>
          spark.read.csv(strPath)
        case "txt" =>
          spark.read.text(strPath)
        case "avro" =>
          spark.read.format("avro").load(strPath)
      }
    sourceDataFrame
  }
}
