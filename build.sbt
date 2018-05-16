organization := "com.whisk"

name := "finagle-kubernetes-resolver"

version := "0.1.0"

scalaVersion := "2.12.6"

scalacOptions ++= Seq("-feature", "-deprecation", "-language:implicitConversions")

publishMavenStyle := true

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
  "com.twitter" %% "finagle-http" % "18.5.0",
  "io.circe" %% "circe-core" % "0.9.3"
)