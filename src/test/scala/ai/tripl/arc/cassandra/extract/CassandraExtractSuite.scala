package ai.tripl.arc.cassandra.extract

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import collection.JavaConverters._
import org.apache.http.client.methods.HttpDelete
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.cassandra.util.TestUtils
import ai.tripl.arc.util.log.LoggerFactory
import ai.tripl.arc.util._
import ai.tripl.arc.util.ControlUtils._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

class CassandraExtractSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _

  val testData = getClass.getResource("/akc_breed_info.csv").toString
  val outputView = "actual"
  val keyspace = "test"
  val table = "dogs"
  val cassandraHost = "cassandra"
  val port = "9042"
  val localDc = "datacenter1"
  val ssl = "false"

  before {
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.port", "9999")
      .appName("Spark ETL Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("FATAL")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
  }

  after {
    session.stop
  }

  test("CassandraExtract") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df0 = spark.read.option("header","true").csv(testData)
    df0.createOrReplaceTempView("expected")

    df0.write
      .format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.host", cassandraHost)
      .option("spark.cassandra.connection.port", port)
      .option("spark.cassandra.connection.local_dc", localDc)
      .option("confirm.truncate", true)
      .options(Map("keyspace" -> keyspace, "table" -> table))
      .mode("overwrite")
      .save()

    CassandraExtractStage.execute(
      CassandraExtractStage(
        plugin=new CassandraExtract,
        name="df",
        description=None,
        keyspace=keyspace,
        table=table,
        outputView=outputView,
        numPartitions=None,
        params=Map(),
        partitionBy=Nil,
        persist=true
      )
    )

    // reselect fields to ensure correct order
    val expected = spark.sql(s"""
    SELECT breed, height_high_inches, height_low_inches, weight_high_lbs, weight_low_lbs FROM expected
    """)

    // reselect fields to ensure correct order
    val actual = spark.sql(s"""
    SELECT breed, height_high_inches, height_low_inches, weight_high_lbs, weight_low_lbs FROM ${outputView}
    """)

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(148, false)
      println("expected")
      expected.show(148, false)
    }
    assert(actualExceptExpectedCount === 0)
    assert(expectedExceptActualCount === 0)
  }

}
