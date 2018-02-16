# spark-base

Spark application base containing utilities for development and tests

## Logging

Logging in spark is a bit tricky, instead of using prints all over the place, because loggers are not serializable, we 
can use a workaround like this :
```scala
case class Logger(name: String) extends Serializable {
  @transient lazy val log = org.apache.log4j.LogManager.getLogger(name)
}
```

and then on workers closures 

```scala
val dumpRdd = sparkContext.parallelize(1 to 100, 2)
                .map(e => Logger("Worker").log.info("Ping!"))
                .collect()
```

## Contexts and application

### Wrapper function to expose contexts
- These wrappers initiates contexts and execute a block of code inside a try closure and ensures that sparkContext is closed at the end 
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
#### Spark and Hive
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
  withSparkContext("MyJob") { (sparkContext, hiveContext, logger) ⇒
    /* Your code here */
  }
}
```
#### Spark and Sql
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
  withSparkContext("MyJob") { (sparkContext, sqlContext, logger) ⇒
    /* Your code here */
  }
}
```
#### Spark, Hive and Sql
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
  withSparkContext("MyJob") { (sparkContext, sqlContext, hiveContext, logger) ⇒
    /* Your code here */
  }
}
```

## Testing

- [ ] TODO