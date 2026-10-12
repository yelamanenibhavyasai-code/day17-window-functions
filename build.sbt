
ThisBuild / version := "0.1.0"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "day17-window-functions",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.3"
    ),
    Compile / run / fork := true,
    Compile / run / outputStrategy := Some(StdoutOutput),
    Compile / run / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
