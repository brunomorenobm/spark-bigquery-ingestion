# Spark Data Warehouse Ingestion Tool Bigquery

Spark based Data Warehouse Ingestion Tool, load an entire database into Google Bigquery with one job, without any code.

#### Data Sources:
* MySQL
* Postgre SQL
* MongoDB
* Google Storage Files:
    * XML
    * JSON
    * CSV
    * XLS
    * XLSX
    * AVRO
    * PARQUET
    * TXT
    
#### Stack
* *Language:* Scala
* *Cloud Provider:* Google Cloud
* *Data Processing Engine:* Spark 2.4
* *Spark-runtime:* Google DataProc

#### Runtime
It runs on top of any Spark Cluster, to make things cheaper and easier, it may run on top of [Google Dataproc](https://cloud.google.com/dataproc), using dynamic allocation, please read the workflow section.


# Requirements
* [Google Cloud console tool](https://cloud.google.com/sdk)

#### How to run
1. To run on any Spark Cluster, please generate the jar file, using the command:

` sbt clean assembly`

###### Run on Google Dataproc
2. Upload the jar file to the [Google Storage](https://cloud.google.com/storage) folder `gs://<your-googlecloud-project-name>/lib/spark-bigquery-ingestion.jar`

3. Change the file [ingestion-workflow.yml](workflows/ingestion-workflow.yml) replacing `your-googlecloud-project-name>` for your google project name.

4. Configure all your datasources, please read [setup datasources](docs/setup-datasources.md)

4. Create all databases referenced in your datasources, using the same region of the Dataproc cluster.

5. Run the command `gcloud auth login` only for the first time.

6. Execute o script [execute-load.sh](bin/execute-load.sh)


#### Extend the Jdbc IoLoader
If you need to include a new RDBMS such Oracle or MSSQL, add a new Query for the RDBMS you may need in method in `createQueryMap` and `createDriverMap` in class [Jdbc](src/main/scala/br/com/bruno/data/ingestion/io/Jdbc.scala)

#### Add new Data Source

If you need to add a new Data Source like a new API ou any other direct approach, follow the steps below:
 
1.  Implement a new [IoLoader](src/main/scala/br/com/bruno/data/ingestion/io/loader/IoLoader.scala) examples: [package](src/main/scala/br/com/bruno/data/ingestion/io)

2.  Add new IoLoader in Class Factory Class: [IoLoaderFactory](src/main/scala/br/com/bruno/data/ingestion/io/loader/IoLoaderFactory.scala/)

#### Future features
Incremental ingestion

#### External Resources

Data Storage on Google Big Query, Spotify: https://github.com/spotify/spark-bigquery

#### License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0




