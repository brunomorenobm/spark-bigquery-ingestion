package br.com.bruno.data.ingestion.model

import org.joda.time.DateTime

class Table (var name: String) extends Serializable {

  var database : String  = null
  var schema : String = null
  var columns : String = null
  var primaryKey : String = null
  var objectType : String = null
  var size = 0L

  var lastExecutionTime : DateTime = null
  var lastExecutionColumn : String = null
  var primaryKeyColumn : String = null
  var lastPrimaryKeyValue : String = null

}
