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

## Testing

- [ ] TODO

## Contexts and application

- [ ] TODO