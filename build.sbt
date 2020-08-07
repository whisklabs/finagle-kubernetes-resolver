val finagleRev = "20.7.0"

val circeRev = "0.13.0"

organization := "com.whisk"

name := "finagle-kubernetes-resolver"

version := "0.2.0"

scalaVersion := "2.12.12"

crossScalaVersions := Seq("2.13.3", "2.12.12")

scalacOptions ++= Seq("-feature", "-deprecation", "-language:implicitConversions")

publishMavenStyle := true

sonatypeProfileName := "com.whisk"

licenses += ("MIT", url("http://opensource.org/licenses/MIT"))

homepage := Some(url("https://github.com/whisklabs/finagle-kubernetes-resolver"))

scmInfo := Some(
  ScmInfo(
    url("https://github.com/whisklabs/finagle-kubernetes-resolver"),
    "scm:git:github.com/whisklabs/finagle-kubernetes-resolver.git"
  )
)

developers := List(
  Developer(id = "viktortnk",
            name = "Viktor Taranenko",
            email = "viktortnk@gmail.com",
            url = url("https://finelydistributed.io/")),
  Developer(id = "vxx1",
            name = "Vladimir Vershinin",
            email = "werschinin@gmail.com",
            url = url("https://github.com/vxx1"))
)

publishTo := Some(Opts.resolver.sonatypeStaging)

libraryDependencies ++= Seq(
  "com.twitter" %% "finagle-http" % finagleRev,
  "io.circe" %% "circe-core" % circeRev,
  "io.circe" %% "circe-parser" % circeRev,
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
)
