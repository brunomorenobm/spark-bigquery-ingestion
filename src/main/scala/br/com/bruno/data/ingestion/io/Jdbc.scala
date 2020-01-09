package br.com.bruno.data.ingestion.io

import java.sql.{Connection, DriverManager}

import br.com.bruno.data.ingestion.Utils
import br.com.bruno.data.ingestion.io.loader.IoLoader
import br.com.bruno.data.ingestion.model.{Step, Table}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


class Jdbc(step: Step) extends IoLoader(step) {

  val EXTRACT_DATABASE_INFO_REGEX = "^(jdbc\\:)(.*)(\\:\\/\\/).*\\/([a-zA-Z]*)(.*)?"
  val rdbms: String = Utils.extractRegexGroup(EXTRACT_DATABASE_INFO_REGEX, step.url, 2)
  val jdbcDriver = createDriverMap.get(rdbms).get
  val queryAllTables = createQueryMap.get(rdbms).get
  step.sourceDatabase = Utils.extractRegexGroup(EXTRACT_DATABASE_INFO_REGEX, step.url, group = 4)


  /**
    *
    * @return
    */
  def discovery: mutable.ListBuffer[Table] = {
    var tables = new ListBuffer[Table]
    val queryAllTable: String = queryAllTables

    var connection: Connection = null

    logger.info(s"Creating connection on for Discovery Process ${step.url}")
    connection = DriverManager.getConnection(step.url, step.user, step.password)
    logger.info("Source Database Connected with String: " + step.url)
    val statement = connection.createStatement
    val resultSet = statement.executeQuery(queryAllTable)
    logger.info("Query returned, starting loading records")
    while (resultSet.next) {
      val table = new Table(resultSet.getString("table_name"))
      table.database = resultSet.getString("database_name")
      table.schema = resultSet.getString("table_schema")
      table.primaryKey = resultSet.getString("primary_key")
      table.columns = resultSet.getString("column_name")
      table.objectType = resultSet.getString("table_type")
      tables += table
    }
    // Filter only Tables inside the Database
    logger.info(s"${tables.size} tables before filtering" )
    val filtered = tables.filter(table => table.database.equalsIgnoreCase(step.sourceDatabase))
    if (filtered.size > 0) {
      tables = filtered
      logger.info(s"${tables.size} tables after filtering" )
    } else {
      logger.info(s" The filter has filtered all tables, getting the original list to process, source Database to filter ${step.sourceDatabase}" )
      tables.foreach(table => println(s"Database: ${table.database} Schema: ${table.schema} Table: ${table.name}"))
    }
    tables
  }

  def loadTable(table: Table): DataFrame = {
    println("Table %s ", table.name)
    val spark = SparkSession.getDefaultSession.get
    val sourceDataFrame: DataFrame = spark.read
      .format("jdbc")
      .option("url", step.url)
      .option("dbtable", table.name)
      .option("user", step.user)
      .option("password", step.password)
      .option("driver", jdbcDriver)
      .load()
    sourceDataFrame
  }


  private def createQueryMap = {
    val queries = new mutable.HashMap[String, String]()
    // TODO: Add to Read From File
    queries.put("postgresql", "select table_catalog database_name,\n" + "       table_name                       table_name,\n" + "       table_type                       table_type,\n" + "       table_schema                     table_schema,\n" + "       array_to_string( array(select kcu.column_name::text\n" + "                       from information_schema.table_constraints tc\n" + "                       JOIN information_schema.key_column_usage AS kcu\n" + "                       ON tc.constraint_name = kcu.constraint_name\n" + "                              AND tc.table_schema = kcu.table_schema\n" + "                              AND tbl.table_schema = kcu.table_schema\n" + "                              AND tc.table_name = tbl.table_name\n" + "                       where constraint_type = 'PRIMARY KEY'\n" + "                       ORDER BY 1),',') primary_key,\n" + "       array_to_string( array(SELECT column_name::text\n" + "                              FROM\n" + "                                     information_schema.COLUMNS col\n" + "                              where\n" + "                                tbl.table_schema = col.table_schema\n" + "                                AND tbl.table_name = col.table_name\n" + "                                AND tbl.table_schema = col.table_schema\n" + "         ),',') column_name\n" + "\n" + "from information_schema.tables tbl " + "where tbl.table_schema = 'public' ")
    // TODO: Add more columns in MySQL
    queries.put("mysql", "SELECT table_schema   database_name,\n       table_name,\n       '' table_schema,\n        '' primary_key,\n       '' column_name,\n       table_type,\n       table_rows table_size\nFROM information_schema.tables\nWHERE table_schema\n        not in ('sys', 'performance_schema', 'my_sql', 'information_schema')")
    // TODO: Add others RDBMS
    queries
  }

  private def createDriverMap: mutable.HashMap[String, String] = {
    val drivers = new mutable.HashMap[String, String]()
    drivers.put("postgresql", "org.postgresql.Driver")
    drivers.put("mysql", "com.mysql.jdbc.Driver")
    drivers
  }
}
