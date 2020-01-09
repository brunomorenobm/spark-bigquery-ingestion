package br.com.bruno.data.ingestion.model

class Step extends Serializable {
  var sourceType: String = null
  var url: String = null
  var user: String = null
  var password: String = null
  var skipDiscovery: Boolean = false
  var targetDatabase: String  = null
  var executionTimeoutInMinutes: Int = 30
  var sourceDatabase: String = null
  var threads: Int = 0
}
