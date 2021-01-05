import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "3.0.1"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"

  // arc
  val arc = "ai.tripl" %% "arc" % "3.7.0" % "provided"

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"
  val sparkHive = "org.apache.spark" %% "spark-hive" % sparkVersion % "provided"

  // cassandra
  val cassandra = "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.0"
  val jsr166e = "com.twitter" % "jsr166e" % "1.1.0" % "provided"

  // Project
  val etlDeps = Seq(
    scalaTest,

    arc,

    sparkSql,
    sparkHive,

    cassandra,
    jsr166e
  )
}
