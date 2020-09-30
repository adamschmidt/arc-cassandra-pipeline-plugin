package ai.tripl.arc.execute

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

  val version = ai.tripl.arc.cassandra.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "inputURI" :: "authentication" :: "params" :: "password" :: "sqlParams" :: "user" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val authentication = readAuthentication("authentication")
    val parsedURI = getValue[String]("inputURI") |> parseURI("inputURI") _
    val inputSQL = parsedURI |> textContentForURI("inputURI", authentication) _
    val sqlParams = readMap("sqlParams", c)
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, parsedURI, inputSQL, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(parsedURI), Right(inputSQL), Right(invalidKeys)) =>
        val stage = CassandraExecuteStage(
          plugin=this,
          id=id,
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
        val allErrors: Errors = List(id, name, description, parsedURI, inputSQL, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class CassandraExecuteStage(
    plugin: CassandraExecute,
    id: Option[String],
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

object CassandraExecuteStage {

  private def resolveHost(hostName: String): Option[InetAddress] = {
    try Some(InetAddress.getByName(hostName))
    catch {
      case NonFatal(e) =>
        None
    }
  }

  def execute(stage: CassandraExecuteStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    // replace sql parameters
    val sql = SQLUtils.injectParameters(stage.sql, stage.sqlParams, false)
    stage.stageDetail.put("sql", sql)

    // get connection and try to execute statement
    try {
      val sparkConf = spark.sparkContext.getConf
      stage.params.foreach { case (key, value) => sparkConf.set(key, value) }
      val cassandraConnectorConf = CassandraConnectorConf.fromSparkConf(sparkConf)
      val connection = CassandraConnector(cassandraConnectorConf)
      connection.withSessionDo(session => session.execute(sql))

    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    None
  }

}