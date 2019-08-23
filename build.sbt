resolvers += "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

name := "spark-streaming-hints"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.4.3"

// sparkComponents ++= Seq("sql", "streaming")
// spDependencies += "com.datastax.spark/spark-cassandra-connector_2.11:2.4.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3", // For Kafka
  "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0" // For Cassandra
)

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
  case "log4j.properties" => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf" => MergeStrategy.concat
  case _ => MergeStrategy.first
}

