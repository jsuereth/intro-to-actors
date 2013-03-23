name := "intro-to-actors"

organization := "com.jsuereth"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.1.0",
  "org.specs2" %% "specs2" % "1.14" % "test"
)

scalacOptions ++= Seq("-feature", "-deprecation", "-optimise")

