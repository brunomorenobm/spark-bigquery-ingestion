package br.com.bruno.data.ingestion.io.loader

import br.com.bruno.data.ingestion.io.{GoogleStorage, Jdbc, MongoDb}
import br.com.bruno.data.ingestion.model.Step


object IoLoaderFactory {


  def createIoLoader(step: Step): IoLoader = {
    step.sourceType match {
      case "jdbc" => new Jdbc(step)
      case "mongodb" => new MongoDb(step)
      case "gs" => new GoogleStorage(step)
    }
  }
}
