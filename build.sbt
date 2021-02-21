name := "s3aws"

version := "0.1"

scalaVersion := "2.13.3"

autoCompilerPlugins := true
addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1")

libraryDependencies ++= Seq(
  "org.typelevel" %% "cats-core" % "2.1.1",
  "org.typelevel" %% "cats-effect" % "2.1.4",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.860"
)
