name := "spark-code-challenge"

version := "0.1"

scalaVersion := "2.12.15"

val sparkVersion = "3.2.1"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.13.2"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.2.2"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % "2.13.2"


// test dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",

  "org.typelevel" %% "cats-effect" % "3.3.11",
  "com.typesafe.akka" %% "akka-actor-typed" % "2.6.19",
  "com.typesafe.akka" %% "akka-stream" % "2.6.19",
  "com.typesafe.akka" %% "akka-http" % "10.2.9",

  "com.typesafe.play" % "play-json_2.12" % "2.8.2" % "test",
  // test dependencies
  "com.holdenkarau" %% "spark-testing-base" % "3.2.0_1.1.1" % "test" exclude ("com.fasterxml.jackson.core","jackson-annotations")
    exclude ("com.fasterxml.jackson.core","jackson-core") exclude ("com.fasterxml.jackson.core","jackson-databind"),
  "org.scalatest" %% "scalatest" % "3.2.11" % "test"
)

resolvers += Resolver.typesafeRepo("releases")
