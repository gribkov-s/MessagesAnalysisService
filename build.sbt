name := "MessagesAnalysisService"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "3.1.1" //2.4.0

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "org.apache.spark" % "spark-streaming-kafka-0-10_2.12" % sparkVersion,
  "org.apache.spark" % "spark-sql-kafka-0-10_2.12" % sparkVersion,
  //"org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.kafka" % "kafka-clients" % "2.6.0",
  "org.apache.kafka" %% "kafka-streams-scala" % "2.6.0",
  "io.confluent" % "kafka-avro-serializer" % "5.5.0",
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.0.4",
  "com.sksamuel.avro4s" %% "avro4s-kafka" % "4.0.4",
  //"com.fasterxml.jackson.core" % "jackson-core" % "2.8.4",
  //"com.fasterxml.jackson.core" % "jackson-databind" % "2.8.4",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.10.2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "net.liftweb" % "lift-json_2.12" % "3.4.0",
  "org.postgresql" % "postgresql" % "42.2.10"
)

assemblyMergeStrategy in assembly := {
  case "META-INF/services/org.apache.spark.sql.sources.DataSourceRegister" => MergeStrategy.concat
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.last
}