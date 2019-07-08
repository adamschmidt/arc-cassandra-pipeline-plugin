package ai.tripl.arc.cassandra.execute

import java.net.{InetAddress, URI}

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.{DetailException, SQLUtils, Utils}
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.util.Logging
import org.apache.spark.sql._

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class CassandraExecute extends PipelineStagePlugin {

  val version = Utils.getFrameworkVersion

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputURI" :: "authentication" :: "params" :: "password" :: "sqlParams" :: "user" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")
    val parsedURI = getValue[String]("inputURI") |> parseURI("inputURI") _
    val inputSQL = parsedURI |> textContentForURI("inputURI", authentication) _
    val sqlParams = readMap("sqlParams", c)
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, parsedURI, inputSQL, invalidKeys) match {
      case (Right(name), Right(description), Right(parsedURI), Right(inputSQL), Right(invalidKeys)) =>
        val stage = CassandraExecuteStage(
          plugin=this,
          name=name,
          description=description,
          inputURI=parsedURI,
          sql=inputSQL,
          sqlParams=sqlParams,
          params=params
        )

        stage.stageDetail.put("inputURI", parsedURI.toString)
        stage.stageDetail.put("sql", inputSQL)
        stage.stageDetail.put("sqlParams", sqlParams.asJava)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, parsedURI, inputSQL, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class CassandraExecuteStage(
                             plugin: CassandraExecute,
                             name: String,
                             description: Option[String],
                             inputURI: URI,
                             sql: String,
                             sqlParams: Map[String, String],
                             params: Map[String, String]
                           ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    CassandraExecuteStage.execute(this)
  }
}

object CassandraExecuteStage extends Logging {

  private def resolveHost(hostName: String): Option[InetAddress] = {
    try Some(InetAddress.getByName(hostName))
    catch {
      case NonFatal(e) =>
        logError(s"Unknown host '$hostName'", e)
        None
    }
  }

  def execute(stage: CassandraExecuteStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    val hosts = for {
      hostName <- stage
        .params.getOrElse("spark.cassandra.connection.host", "localhost")
        .split(",").toSet[String]
      hostAddress <- resolveHost(hostName.trim)
    } yield hostAddress

    val port = stage.params.getOrElse("spark.cassandra.connection.port", "9042").toInt
    val localDC = stage.params.get("spark.cassandra.connection.local_dc")

    // replace sql parameters
    val sql = SQLUtils.injectParameters(stage.sql, stage.sqlParams, false)
    stage.stageDetail.put("sql", sql)

    // get connection and try to execute statement
    try {

      val connection = CassandraConnector(hosts, port=port, localDC=localDC)
      connection.withSessionDo(session => session.execute(sql))

    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    None
  }

}