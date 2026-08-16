name := "order-generator"
version := "1.0"
scalaVersion := "2.13.12"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.6.1"

// Fixed, predictable output name so the Dockerfile doesn't have to guess
// a version-suffixed filename.
assembly / assemblyJarName := "order-generator.jar"

// kafka-clients and its transitive deps ship some overlapping META-INF
// files (license notices, etc.) that would otherwise cause a
// "deduplicate" error during `sbt assembly`.
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", _*) => MergeStrategy.discard
  case _                        => MergeStrategy.first
}
