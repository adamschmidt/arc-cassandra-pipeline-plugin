package ai.tripl.arc.cassandra.execute

import java.net.URI

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter
import org.apache.commons.io.FileUtils
import org.apache.commons.io.IOUtils
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.cassandra.util.TestUtils
import ai.tripl.arc.util._

class CassandraExecuteSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  var connection: java.sql.Connection = _

  val outputView = "dataset"
  var testURI = FileUtils.getTempDirectoryPath()
  var cassandraHost = "cassandra"

  before {
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .config("spark.ui.port", "9999")
      .appName("Spark ETL Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    implicit val logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._

  }

  after {
    session.stop
  }

  test("CassandraExecute: server succeed") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)


    val transaction = s"""
    CREATE KEYSPACE IF NOT EXISTS test WITH replication={'class':'SimpleStrategy', 'replication_factor':1}
    """.stripMargin

    ai.tripl.arc.cassandra.execute.CassandraExecuteStage.execute(
      ai.tripl.arc.cassandra.execute.CassandraExecuteStage(
        plugin=new ai.tripl.arc.cassandra.execute.CassandraExecute,
        name=outputView,
        description=None,
        inputURI=new URI(testURI),
        sql=transaction,
        params=Map("spark.cassandra.connection.host" -> cassandraHost),
        sqlParams=Map.empty
      )
    )

  }

  test("CassandraExecute: server failure statement") {
    implicit val spark = session
    import spark.implicits._
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val transaction = s"""
    HELP SHOW;
    """.stripMargin

    val thrown = intercept[Exception with DetailException] {
      ai.tripl.arc.cassandra.execute.CassandraExecuteStage.execute(
        ai.tripl.arc.cassandra.execute.CassandraExecuteStage(
          plugin=new ai.tripl.arc.cassandra.execute.CassandraExecute,
          name=outputView,
          description=None,
          inputURI=new URI(testURI),
          sql=transaction,
          params=Map("spark.cassandra.connection.host" -> cassandraHost),
          sqlParams=Map.empty
        )
      )
    }
    assert(thrown.getMessage.contains("""no viable alternative at input 'HELP'"""))
  }
}