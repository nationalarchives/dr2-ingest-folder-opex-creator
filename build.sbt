import Dependencies._
import uk.gov.nationalarchives.sbt.Log4j2MergePlugin.log4j2MergeStrategy

ThisBuild / organization := "uk.gov.nationalarchives"
ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file(".")).settings(
  name := "dr2-ingest-folder-opex-creator",
  libraryDependencies ++= Seq(
    fs2Reactive,
    log4jSlf4j,
    log4jCore,
    log4jTemplateJson,
    lambdaCore,
    pureConfig,
    pureConfigCats,
    scalaXml,
    upickle,
    dynamoClient,
    s3Client,
    scalaTest % Test,
    wiremock % Test
  ),
  scalacOptions += "-deprecation"
)
(assembly / assemblyJarName) := "dr2-ingest-folder-opex-creator.jar"

scalacOptions ++= Seq("-Wunused:imports", "-Werror")

(assembly / assemblyMergeStrategy) := {
  case PathList(ps @ _*) if ps.last == "Log4j2Plugins.dat" => log4j2MergeStrategy
  case _                                                   => MergeStrategy.first
}
