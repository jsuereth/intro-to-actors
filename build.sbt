name := "intro-to-actors"

organization := "com.jsuereth"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" % "akka-actor" % "2.0.1",
  "org.specs2" %% "specs2" % "1.10" % "test"
)

