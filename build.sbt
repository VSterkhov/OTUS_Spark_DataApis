ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "OTUS_Spark_DataApis",
    assembly / assemblyJarName := "OTUS_Spark_DataApis.jar",
    idePackagePrefix := Some("ru.broom")
  )

val sparkVersion = "3.3.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.postgresql" % "postgresql" % "42.4.3"
)

// https://index.scala-lang.org/permutive/sbt-liquibase-plugin
// import com.permutive.sbtliquibase.SbtLiquibase
// enablePlugins(SbtLiquibase)
// liquibaseUsername := "postgres"
// liquibasePassword := "postgres"
// liquibaseDriver   := "org.postgresql.Driver"
// liquibaseUrl      := "jdbc:postgresql://uhadoop.localdomain:5432/spark_views?createDatabaseIfNotExist=true"
// liquibaseChangelog := new File("./liquibase/changelog.xml")