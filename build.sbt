
name := "SparkTests"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= Seq(
  "org.apache.spark"  %%  "spark-core"    % "2.4.5"   % "provided",
  "org.apache.spark"  %%  "spark-sql"     % "2.4.5",
  "org.apache.spark"  %%  "spark-mllib"   % "2.4.5"
)