## Basic Transformations

## Pre-requisites
Please make sure you have the following installed
* Java 8
* Scala 2.11
* Sbt 1.1.x
* Apache Spark 2.3 with ability to run spark-submit

## Setup Process
* Clone the repo
* Build: sbt package
* Test: sbt test
* Note: Update src/test/resources/application.conf to change configurations for testing

## Running Data Apps
* Package the project with
``` 
sbt package
``` 
* Sample data is available in the src/test/resource/data directory

### Running Wordcount
By default this app will read from the words.txt file and write to the target folder.  Pass in the input source and output path to wordcount different files. 
```
spark-submit --class thoughtworks.batch.wordcount.WordCount --master local target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar ./src/test/resources/data/words.txt target/test01
```

### Running multi-step pipeline

#### Pipeline structure
* Execute ingest_to_data_lake DailyDriver which will ingest data and write as parquet
* Data applications like Citibike should use data in parquet and write results to a new directory 


#### Useful Commands
You can run these commands from the transformations directory.
* Ingest data from external source to datalake: 
```
spark-submit --class thoughtworks.batch.ingest_to_data_lake.DailyDriver --master local target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar $(INPUT_LOCATION) $(OUTPUT_LOCATION)
```
* Transform Citibike data: 
```
spark-submit --class thoughtworks.batch.citibike.CitibikeTransformer --master local target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar $(INPUT_LOCATION) $(OUTPUT_LOCATION)
```

* Sample App: 
Remember to change the configurations in src > main > resources > application.conf before running this command.
```
spark-submit --jars ../config-1.3.2.jar --class thoughtworks.batch.app.UberRidesByHumidityRange --master local target/scala-2.11/tw-pipeline_2.11-0.1.0-SNAPSHOT.jar
```

