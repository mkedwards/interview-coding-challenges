name := "spark-code-challenge"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.2.0"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7"
dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.7"


// test dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion  % "provided",

  "com.typesafe.play" % "play-json_2.11" % "2.5.18" % "test",
  // test dependencies
  "com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % "test" exclude ("com.fasterxml.jackson.core","jackson-annotations")
    exclude ("com.fasterxml.jackson.core","jackson-core") exclude ("com.fasterxml.jackson.core","jackson-databind"),
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"
)

resolvers += Resolver.typesafeRepo("releases")