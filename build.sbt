name := "spark-base"

version := "0.1"

scalaVersion := "2.10.6"

val sparkVersion = "1.6.2"
val elasticSearchVersion = "2.4.2"


resolvers ++= Seq(
  "elasticsearch-releases" at "http://maven.elasticsearch.org/releases",
  "hortonworks-releases" at "http://repo.hortonworks.com/content/repositories/releases/"
)

val testingDependencies = Seq(
  "com.holdenkarau" %% "spark-testing-base" % "1.6.2_0.7.4" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test,
  "pl.allegro.tech" % "embedded-elasticsearch" % elasticSearchVersion % Test

)
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.elasticsearch" %% "elasticsearch-spark" % elasticSearchVersion,
  "org.elasticsearch" % "elasticsearch" % elasticSearchVersion
) ++ testingDependencies