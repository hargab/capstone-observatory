name := course.value ++ "-" ++ assignment.value

scalaVersion := "2.11.8"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-encoding", "UTF-8",
  "-unchecked",
  "-Xlint",
  "-Yno-adapted-args",
  "-Ywarn-dead-code",
  "-Ywarn-value-discard",
  "-Xfuture",
  "-Xexperimental"
)

val sparkVersion = "2.1.0"

libraryDependencies ++= Seq(
  "com.sksamuel.scrimage" %% "scrimage-core" % "2.1.6", // for visualization
  // You don’t *have to* use Spark, but in case you want to, we have added the dependency
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" % "spark-hive_2.10" % sparkVersion,
  // You don’t *have to* use akka-stream, but in case you want to, we have added the dependency
  //"com.typesafe.akka" %% "akka-stream" % "2.4.12",
  //"com.typesafe.akka" %% "akka-stream-testkit" % "2.4.12" % Test,
  // You don’t *have to* use Monix, but in case you want to, we have added the dependency
  //"io.monix" %% "monix" % "2.1.1",
  // You don’t *have to* use fs2, but in case you want to, we have added the dependency
  //"co.fs2" %% "fs2-io" % "0.9.2",
  "org.scalacheck" %% "scalacheck" % "1.12.1" % Test,
  //"junit" % "junit" % "4.10" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalactic" %% "scalactic" % "3.0.1",
  "com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % "test"
)

courseId := "PCO2sYdDEeW0iQ6RUMSWEQ"

assignmentsMap := Map(
  "observatory" -> Assignment(
    packageName = "observatory",
    key = "l1U9JXBMEea_kgqTjVyNvw",
    itemId = "Cr2wv",
    partId = "CWoWG",
    maxScore = 10d,
    styleScoreRatio = 0.2,
    styleSheet = (baseDirectory.value / "scalastyle" / "observatory.xml").getPath,
    options = Map("Xmx" -> "1600m", "grader-memory" -> "2048", "grader-cpu" -> "2")
  )
)

parallelExecution in Test := false // So that tests are executed for each milestone, one after the other

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")