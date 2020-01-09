# Data Source Configuration
Here you find more details related to how to setup a new Spark Job as a Step.

## Data Source Types:

* [Google Storage](/src/main/scala/br/com/bruno/data/ingestion/io/GoogleStorage.scala) | gs
    * It reads all subfolders of an Google Storage folder, if any subfolder contains one or more data files (JSON, XML, CSV, XLS, TXT, XSLX, AVRO, PARQUET), the component automatically creates a new table with the folder's name. 
* [MongoDB](/src/main/scala/br/com/bruno/data/ingestion/io/MongoDb.scala) | mongodb 
    * The component reads all collections from given database, and creates an internal thread to ingest the data
* [JDBC](/src/main/scala/br/com/bruno/data/ingestion/io/GoogleStorage.scala) | jdbc
    * The component reads all tables containg in the given database, creates an thread for each table and ingest the data into the Google Storage table.

## Examples:
File: [ingestion-workflow.yml](workflows/ingestion-workflow.yml)
### JDBC
``` yaml
# https://cloud.google.com/dataproc/docs/concepts/workflows/using-yamls
jobs:
  - sparkJob:
      args:
        - jdbc # Data Source Type
        - jdbc:postgresql://host:port/<database-name> # Source DB JDBC URL
        - <username> # Source DB username
        - <password> # Source DB Password
        - <your-googlecloud-project-name>:<google-bigquery-database-name>  # Target Big Query Database
        - '30' # Timeout in minutes to execute the Ingestion
        - '25' # Number of Component internal Threads, default 20
      jarFileUris:
        - gs://<your-googlecloud-project-name>/lib/igt_database.jar # Project Jarfile, saved in Google Storage
        - gs://<your-googlecloud-project-name>/lib/postgresql-42.2.5.jar # JDBC Jar file, saved in Google Storage
      mainClass: br.com.bruno.data.ingestion.Starter  # Scala Main Class
      properties: # Read more https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/cluster-properties
        spark.executor.cores: '5'
        spark.executor.memory: 4g
    stepId: jdbc_step
  - sparkJob:
      args:
        - gs # Google Storage
        - <your-googlecloud-project-name>/stg/cna/data/landing/ # Source Google Storage root folder, it will read all sub-folders, if exists one or more data files (json, xml, csv) in any subfolder it will create a table and load the content into the new table
        - "*" # Username use *
        - "*" # password use *
        - <your-googlecloud-project-name>:<google-bigquery-database-name> # Target
        - '30' # Timeout to execute the data Load
        - '1' # Number of Component internal Threads, default 20
      jarFileUris:
        - gs://<your-googlecloud-project-name>/lib/igt_database.jar # Project Jarfile, saved in Google Storage
      mainClass: br.com.bruno.data.ingestion.Starter # Scala Main Class
      properties: # Read more https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/cluster-properties
        spark.executor.cores: '4'
        spark.executor.memory: 4g
    stepId: google_storage
- sparkJob:
    args:
    - mongodb # Data Source Type
    - mongodb://<user>:<password>@<host>:<port>/<database>?authSource=admin&authMechanism=SCRAM-SHA-1 # Source DB URL
    - <username> # Source DB username
    - <password> # Source DB Password
    - <your-googlecloud-project-name>:<google-bigquery-database-name> # Target Big Query Database
    - '30' # Timeout in minutes to execute the Ingestion
    - '25' # Number of Component internal Threads, default 20
    jarFileUris:
    - gs://<your-googlecloud-project-name>/lib/igt_database.jar # Project Jarfile, saved in Google Storage
    mainClass: br.com.bruno.data.ingestion.Starter # Scala Main Class
    properties: # Read more https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/cluster-properties
      spark.executor.cores: '5'
      spark.executor.memory: 4g
  stepId: mongodb_step
placement: # Read more: https://cloud.google.com/dataproc/docs/concepts/workflows/using-yamls
  managedCluster:
    clusterName: db-ingestion-cluster-temp
    config:
      gceClusterConfig:
        zoneUri: us-east1-b
        networkUri: projects/<your-googlecloud-project-name>/global/networks/default
      masterConfig:
        diskConfig:
          bootDiskSizeGb: 30
          bootDiskType: pd-standard
        machineTypeUri: n1-standard-8
      softwareConfig:
        properties:
          dataproc:dataproc.allow.zero.workers: 'true'
          dataproc:dataproc.monitoring.stackdriver.enable: 'true'
``