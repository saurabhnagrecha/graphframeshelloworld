name := "GraphFramesTesting"

version := "0.1"

scalaVersion := "2.11.12"
val sparkVersion = "2.3.0"

resolvers += Resolver.url("SparkPackages", url("https://dl.bintray.com/spark-packages/maven/"))

libraryDependencies ++= Seq(
  // spark core
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,

  // spark-modules
  "org.apache.spark" %% "spark-graphx" % sparkVersion,
   "org.apache.spark" %% "spark-mllib" % sparkVersion
)
