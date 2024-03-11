lazy val root = (project in file("."))
   .settings(
       name := "word count",
       version := "1.0",
       scalaVersion := "2.12.17"
   )

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
    "org.apache.spark" %% "spark-sql" % "3.4.1" % "provided"
)

