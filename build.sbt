
name := "ftp-kafka-converters"

version := "1.0"

scalaVersion := "2.11.8"

val kafkaVersion = "0.10.0.1"

libraryDependencies ++= {
  Seq(
    "com.github.nscala-time" %% "nscala-time" % "2.14.0",
    "org.scala-lang" % "scala-xml" % "2.11.0-M4",
    "org.apache.kafka" % "connect-api" % kafkaVersion % "provided",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test"
  )
}

resolvers ++= Seq(
  Resolver.sonatypeRepo("public"),
  "Confluent Maven Repo" at "http://packages.confluent.io/maven/"
)
