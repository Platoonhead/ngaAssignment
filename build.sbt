ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.8"

val AkkaVersion = "2.6.14"

lazy val root = (project in file("."))
  .settings(
    name := "NGA"
  )

libraryDependencies ++= Seq(
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "3.0.4",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "3.0.4",
  "com.typesafe.akka" %% "akka-stream" % AkkaVersion
)