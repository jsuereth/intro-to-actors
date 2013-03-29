name := "intro-to-actors"

organization := "com.jsuereth"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

resolvers += "Oracle repo" at "http://download.oracle.com/maven"

scalaVersion := "2.10.1"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.2-M2",
  "com.typesafe.akka" %% "akka-cluster-experimental" % "2.2-M2",
  "com.typesafe.akka" %% "akka-contrib" % "2.2-M2",
  "org.specs2" %% "specs2" % "1.14" % "test",
  "com.sleepycat" % "je" % "3.3.75",
  "net.databinder" %% "unfiltered-netty-server" % "0.6.7",
  "com.basho.riak" % "riak-client" % "1.1.0"
)

scalacOptions ++= Seq("-feature", "-deprecation", "-optimise")
