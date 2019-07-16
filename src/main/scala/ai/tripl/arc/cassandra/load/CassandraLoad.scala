package ai.tripl.arc.cassandra.load

import ai.tripl.arc.api.API._
import ai.tripl.arc.api._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.config._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.{DetailException, ListenerUtils}
import com.typesafe.config._
import org.apache.spark.sql._

import scala.collection.JavaConverters._


class CassandraLoad extends PipelineStagePlugin {
  
  val version = ai.tripl.arc.cassandra.BuildInfo.version

  override def instantiate(index: Int, config: Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: API.ARCContext): Either[List[Error.StageError], API.PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._

    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "table"  :: "keyspace" :: "output"  :: "numPartitions" :: "partitionBy" :: "saveMode" :: "persist" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val table = getValue[String]("table")
    val keyspace = getValue[String]("keyspace")
    val output = getValue[String]("output")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val saveMode = getValue[String]("saveMode", default = Some("Overwrite"), validValues = "Append" :: "ErrorIfExists" :: "Ignore" :: "Overwrite" :: Nil) |> parseSaveMode("saveMode") _
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, table, keyspace, output, persist, numPartitions, partitionBy, saveMode, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(table), Right(keyspace), Right(output), Right(persist), Right(numPartitions), Right(partitionBy), Right(saveMode), Right(invalidKeys)) =>

        val stage = CassandraLoadStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          table=table,
          keyspace=keyspace,
          output=output,
          params=params,
          numPartitions=numPartitions,
          partitionBy=partitionBy,
          saveMode=saveMode
        )

        stage.stageDetail.put("table", table)
        stage.stageDetail.put("keyspace", keyspace)
        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("output", output)
        stage.stageDetail.put("params", params.asJava)
        stage.stageDetail.put("partitionBy", partitionBy.asJava)
        stage.stageDetail.put("saveMode", saveMode.toString.toLowerCase)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, table, keyspace, output, persist, numPartitions, partitionBy, saveMode, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class CassandraLoadStage(
                                   plugin: CassandraLoad,
                                   name: String,
                                   description: Option[String],
                                   inputView: String,
                                   table: String,
                                   keyspace: String,
                                   output: String,
                                   partitionBy: List[String],
                                   numPartitions: Option[Int],
                                   saveMode: SaveMode,
                                   params: Map[String, String]
                                 ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    CassandraLoadStage.execute(this)
  }
}

object CassandraLoadStage {

  def execute(stage: CassandraLoadStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger): Option[DataFrame] = {

    val df = spark.table(stage.inputView)

    stage.numPartitions match {
      case Some(partitions) => stage.stageDetail.put("numPartitions", Integer.valueOf(partitions))
      case None => stage.stageDetail.put("numPartitions", Integer.valueOf(df.rdd.getNumPartitions))
    }

    val dropMap = new java.util.HashMap[String, Object]()

    val unsupported = df.schema.filter( _.name == "_index").map(_.name)
    if (!unsupported.isEmpty) {
      dropMap.put("Unsupported", unsupported.asJava)
    }

    stage.stageDetail.put("drop", dropMap)

    val nonNullDF = df.drop(unsupported:_*)

    val listener = ListenerUtils.addStageCompletedListener(stage.stageDetail)

    val outputDF = try {
      stage.partitionBy match {
        case Nil =>
          val dfToWrite = stage.numPartitions.map(nonNullDF.repartition(_)).getOrElse(nonNullDF)
          dfToWrite.write
            .options(stage.params)
            .options(Map("table" -> stage.table, "keyspace" -> stage.keyspace))
            .mode(stage.saveMode)
            .format("org.apache.spark.sql.cassandra").save(stage.output)
          dfToWrite
        case partitionBy => {
          // create a column array for repartitioning
          val partitionCols = partitionBy.map(col => nonNullDF(col))
          stage.numPartitions match {
            case Some(n) =>
              val dfToWrite = nonNullDF.repartition(n, partitionCols:_*)
              dfToWrite.write.options(stage.params).partitionBy(partitionBy:_*).mode(stage.saveMode).format("org.apache.spark.sql.cassandra").save(stage.output)
              dfToWrite
            case None =>
              val dfToWrite = nonNullDF.repartition(partitionCols:_*)
              dfToWrite.write.options(stage.params).partitionBy(partitionBy:_*).mode(stage.saveMode).format("org.apache.spark.sql.cassandra").save(stage.output)
              dfToWrite
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    spark.sparkContext.removeSparkListener(listener)

    Option(outputDF)
  }
}
