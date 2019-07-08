package ai.tripl.arc.cassandra.load

import java.net.URI
import java.util.UUID
import java.util.Properties

import ai.tripl.arc.ARC
import org.apache.http.client.methods.HttpDelete
import org.apache.http.impl.client.HttpClientBuilder
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import scala.collection.JavaConverters._
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.cassandra.util.TestUtils
import ai.tripl.arc.util._
import ai.tripl.arc.util.log.LoggerFactory
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

class CassandraLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val testData = getClass.getResource("/akc_breed_info.csv").toString
  val inputView = "expected"
  val keyspace = "test"
  val table = "dogs"
  val cassandraHost = "localhost"
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
    spark.sparkContext.setLogLevel("ERROR")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
  }

  after {
    session.stop()
  }

  test("CassandraLoad") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df0 = spark.read.option("header","true").csv(testData)
    df0.createOrReplaceTempView(inputView)

    CassandraLoadStage.execute(
      CassandraLoadStage(
        plugin=new CassandraLoad,
        name="df",
        description=None,
        inputView=inputView,
        table=table,
        keyspace=keyspace,
        output=table,
        numPartitions=None,
        params=Map(
          "confirm.truncate" -> "true"
        ),
        saveMode=SaveMode.Overwrite,
        partitionBy=Nil
      )
    )

    val df1 = spark.read
      .format("org.apache.spark.sql.cassandra")
      .option("spark.cassandra.connection.host", cassandraHost)
      .option("spark.cassandra.connection.port", port)
      .option("spark.cassandra.connection.local_dc", localDc)
      .options(Map("keyspace" -> keyspace, "table" -> table))
      .load()

    df1.createOrReplaceTempView("actual")


    // reselect fields to ensure correct order
    val expected = spark.sql(s"""
    SELECT breed, height_high_inches, height_low_inches, weight_high_lbs, weight_low_lbs FROM ${inputView}
    """)

    // reselect fields to ensure correct order
    val actual = spark.sql(s"""
    SELECT breed, height_high_inches, height_low_inches, weight_high_lbs, weight_low_lbs FROM actual
    """)

    val actualExceptExpectedCount = actual.except(expected).count
    val expectedExceptActualCount = expected.except(actual).count
    if (actualExceptExpectedCount != 0 || expectedExceptActualCount != 0) {
      println("actual")
      actual.show(100000, false)
      println("expected")
      expected.show(100000, false)
    }
    assert(actualExceptExpectedCount === 0)
    assert(expectedExceptActualCount === 0)
  }


  test("CassandraLoad end-to-end") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = LoggerFactory.getLogger(spark.sparkContext.applicationId)
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val df = spark.read.option("header","true").csv(testData)
    df.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "CassandraLoad",
          "name": "write person",
          "table": "dogs",
          "keyspace": "test",
          "environments": [
            "production",
            "test"
          ],
          "output": "person",
          "inputView": "${inputView}",
          "saveMode": "Overwrite",
          "params": {
            "confirm.truncate": "true",
          }
        }
      ]
    }"""

    val pipelineEither = ConfigUtils.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(_) => {
        println(pipelineEither)
        assert(false)
      }
      case Right((pipeline, _)) => ARC.run(pipeline)(spark, logger, arcContext)
    }
  }
}
