# spark-base

Spark application base containing utilities for development and tests

## Logging

Logging in spark is a bit tricky, instead of using prints all over the place, because loggers are not serializable, we 
can use a workaround like below :
```scala
case class Logger(name: String) extends Serializable {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(name)
}
```

and then on workers closures 

```scala
import fr.s3ni0r.utils.spark.{Utils ⇒ SparkUtils}
import fr.s3ni0r.utils.logger.Logger

object MyJob extends App with SparkUtils {
  withSparkContext("MyJob") { (sparkContext, driverLogger) ⇒
    driverLogger.warn("Job started")
    val dumpRdd = sparkContext.parallelize(1 to 100, 2)
      .map(e => Logger("Worker").log.info("Ping!"))
      .collect()
  }
}
```

## Contexts and application

### Wrapper function to expose contexts
- These wrappers functions initiates contexts and execute a block of code inside a try closure and ensures that sparkContext is closed at the end 
#### Spark context
```scala
def withSparkContext(appName: String = s"Job_${getCurrentDate()}", isLocal: Boolean = false)
      (block: (SparkContext, Logger) ⇒ Any) = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    val conf = isLocal match {
      case true ⇒ new SparkConf().setAppName(s"$appName").setMaster("local[*]")
      case _    ⇒ new SparkConf().setAppName(s"$appName")
    }
    val sparkContext = new SparkContext(conf)
    try {
      block.apply(sparkContext, log)
    } catch {
      case e: Exception ⇒ log.error(e.getMessage)
    } finally {
      sparkContext.stop()
    }
  }
```
- usage
```scala
import fr.s3ni0r.utils.spark.{Utils ⇒ SparkUtils}

object MyJob extends App with SparkUtils {
  withSparkContext("MyJob") { (sparkContext, logger) ⇒
    /* Your code here */
  }
}
```
#### Spark and Hive contexts
```scala
def withSparkHiveContexts(appName: String = s"Job_${getCurrentDate()}", isLocal: Boolean = false)(
      block: (SparkContext, HiveContext, Logger) ⇒ Any) = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    val conf = isLocal match {
      case true ⇒ new SparkConf().setAppName(s"$appName").setMaster("local[*]")
      case _    ⇒ new SparkConf().setAppName(s"$appName")
    }
    val sparkContext = new SparkContext(conf)
    val hiveContext  = new HiveContext(sparkContext)
    try {
      block.apply(sparkContext, hiveContext, log)
    } catch {
      case e: Exception ⇒ log.error(e.getMessage)
    } finally {
      sparkContext.stop()
    }
  }
```
- usage
```scala
import fr.s3ni0r.utils.spark.{Utils ⇒ SparkUtils}

object MyJob extends App with SparkUtils {
  withSparkHiveContexts("MyJob") { (sparkContext, hiveContext, logger) ⇒
    /* Your code here */
  }
}
```
#### Spark and Sql contexts
```scala
def withSparkSqlContexts(appName: String = s"Job_${getCurrentDate()}", isLocal: Boolean = false)(
      block: (SparkContext, SQLContext, Logger) ⇒ Any) = {
    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    val conf = isLocal match {
      case true ⇒ new SparkConf().setAppName(s"$appName").setMaster("local[*]")
      case _    ⇒ new SparkConf().setAppName(s"$appName")
    }
    val sparkContext = new SparkContext(conf)
    val sqlContext   = new SQLContext(sparkContext)
    try {
      block.apply(sparkContext, sqlContext, log)
    } catch {
      case e: Exception ⇒ log.error(e.getMessage)
    } finally {
      sparkContext.stop()
    }
  }
```
- usage
```scala
import fr.s3ni0r.utils.spark.{Utils ⇒ SparkUtils}

object MyJob extends App with SparkUtils {
  withSparkSqlContexts("MyJob") { (sparkContext, sqlContext, logger) ⇒
    /* Your code here */
  }
}
```
#### Spark, Hive and Sql contexts
```scala
def withSparkSqlHiveContexts(appName: String = s"Job_${getCurrentDate()}", isLocal: Boolean = false)(
      block: (SparkContext, SQLContext, HiveContext, Logger) ⇒ Any) = {

    val log = LogManager.getRootLogger
    log.setLevel(Level.WARN)
    val conf = isLocal match {
      case true ⇒ new SparkConf().setAppName(s"$appName").setMaster("local[*]")
      case _    ⇒ new SparkConf().setAppName(s"$appName")
    }

    val sparkContext = new SparkContext(conf)
    val sqlContext   = new SQLContext(sparkContext)
    val hiveContext  = new HiveContext(sparkContext)
    try {
      block.apply(sparkContext, sqlContext, hiveContext, log)
    } catch {
      case e: Exception ⇒ log.error(e.getMessage)
    } finally {
      sparkContext.stop()
    }
  }
```
- usage
```scala
import fr.s3ni0r.utils.spark.{Utils ⇒ SparkUtils}

object MyJob extends App with SparkUtils {
  withSparkSqlHiveContexts("MyJob") { (sparkContext, sqlContext, hiveContext, logger) ⇒
    /* Your code here */
  }
}
```

## Deployment

Let's face it, using web interfaces to upload files into HDFS or to configure and launch a spark job is painful.
in case you have access to HDFS via Webhdfs and to Oozie you can use this `scripts/deploy/deploy.sh` script to :

- Sync your local directory containing the needed files `(Spark job jar, workflow.xml, etc...)` to configure and run a spark job.
- Run the spark job via Oozie
- Open up yarn log history pages for the job spark workflow

> Requirements:
> - [Jq](https://stedolan.github.io/jq/) : to parse json output
> - You need to have access to HDFS via Webhdfs, with of course needed credentials and enough authorisation to do so.

```bash
# These environment variables are needed and will be used in the bash script
export WEBHDFS_URL=https://*********:****/gateway/default/webhdfs/v1
export WEBHDFS_USER=*********
export WEBHDFS_PASSWORD=*********
export OOZIE_URL=https://*********:####/gateway/default/oozie/v1
export OOZIE_USER=*********
export OOZIE_PASSWORD=*********
export BASE_HDFS_DIRECTORY=path/to/work/directory
##############################################################################################
# deploy.sh param1 param2                                                                    #
#   param1: Relative directory to folder containing job files                                #
#   param2: Folder to create in remote HDFS which will be appanded to ${BASE_HDFS_DIRECTORY} #
##############################################################################################
./scripts/deploy/deploy.sh ./job project-x
```

## Testing

- [ ] TODO 