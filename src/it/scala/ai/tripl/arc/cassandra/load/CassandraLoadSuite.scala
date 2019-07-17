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
import ai.tripl.arc.config.ArcPipeline
import ai.tripl.arc.util.log.LoggerFactory
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._

class CassandraLoadSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val testData = getClass.getResource("/akc_breed_info.csv").toString
  val inputView = "expected"
  val outputView = "actual"
  val keyspace = "test"
  val table = "dogs"
  val cassandraHost = "cassandra"
  val port = "9042"
  val localDc = "datacenter1"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
  }

  after {
    session.stop()
  }

  test("CassandraLoad end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val input = spark.read.option("header","true").csv(testData)
    input.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "ai.tripl.arc.execute.CassandraExecute",
          "name": "create table",
          "inputURI": "${getClass.getResource("/create_keyspace.cql").toString}",
          "environments": [
            "production",
            "test"
          ]          
        },        
        {
          "type": "ai.tripl.arc.execute.CassandraExecute",
          "name": "create table",
          "inputURI": "${getClass.getResource("/create_table.cql").toString}",
          "environments": [
            "production",
            "test"
          ]          
        },
        {
          "type": "CassandraLoad",
          "name": "write",
          "table": "dogs",
          "keyspace": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "saveMode": "Overwrite",
          "params": {
            "confirm.truncate": "true",
          }
        },
        {
          "type": "CassandraExtract",
          "name": "read",
          "table": "dogs",
          "keyspace": "test",
          "environments": [
            "production",
            "test"
          ],
          "keyspace": "test",
          "table": "dogs",
          "outputView": "${outputView}"
        }            
      ]
    }"""


    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => {
        println(err)
        assert(false)
      }
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext)
        df match {
          case Some(df) => assert(df.count == input.count)
          case None => assert(false)
        }
      }
    }
  }

}
