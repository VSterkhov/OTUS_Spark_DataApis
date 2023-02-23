ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "OTUS_Spark_DataApis",
    idePackagePrefix := Some("ru.broom")
  )

val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.postgresql" % "postgresql" % "42.4.3"
)

// https://index.scala-lang.org/permutive/sbt-liquibase-plugin
import com.permutive.sbtliquibase.SbtLiquibase
enablePlugins(SbtLiquibase)
liquibaseUsername := ""
liquibasePassword := ""
liquibaseDriver   := "com.mysql.jdbc.Driver"
liquibaseUrl      := "jdbc:mysql://localhost:3306/test_db?createDatabaseIfNotExist=true"