package ai.tripl.arc.extract

import ai.tripl.arc.api.API._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.config._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConverters._

class CassandraExtract extends PipelineStagePlugin {

  val version = ai.tripl.arc.cassandra.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "id" :: "name" :: "description" :: "environments" :: "table"  :: "keyspace" :: "outputView"  :: "numPartitions" :: "partitionBy" :: "persist" :: "params" :: Nil
    val id = getOptionalValue[String]("id")
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val table = getValue[String]("table")
    val keyspace = getValue[String]("keyspace")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val numPartitions = getOptionalValue[Int]("numPartitions")
    val partitionBy = getValue[StringList]("partitionBy", default = Some(Nil))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (id, name, description, table, keyspace, outputView, persist, numPartitions, partitionBy, invalidKeys) match {
      case (Right(id), Right(name), Right(description), Right(table), Right(keyspace), Right(outputView), Right(persist), Right(numPartitions), Right(partitionBy), Right(invalidKeys)) =>

        val stage = CassandraExtractStage(
          plugin=this,
          id=id,
          name=name,
          description=description,
          table=table,
          keyspace=keyspace,
          outputView=outputView,
          params=params,
          persist=persist,
          numPartitions=numPartitions,
          partitionBy=partitionBy
        )

        stage.stageDetail.put("table", table)
        stage.stageDetail.put("keyspace", keyspace)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("params", params.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(id, name, description, table, keyspace, outputView, persist, numPartitions, partitionBy, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

case class CassandraExtractStage(
    plugin: CassandraExtract,
    id: Option[String],
    name: String,
    description: Option[String],
    table: String,
    keyspace: String,
    outputView: String,
    params: Map[String, String],
    persist: Boolean,
    numPartitions: Option[Int],
    partitionBy: List[String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    CassandraExtractStage.execute(this)
  }
}

object CassandraExtractStage {

  def execute(stage: CassandraExtractStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {

    // if incoming dataset is empty create empty dataset with a known schema
    val df = try {
      if (arcContext.isStreaming) {
        spark.emptyDataFrame
      } else {
        spark.read.format("org.apache.spark.sql.cassandra")
          .options(stage.params)
          .options(Map("table" -> stage.table, "keyspace" -> stage.keyspace))
          .load()
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // repartition to distribute rows evenly
    val repartitionedDF = stage.partitionBy match {
      case Nil => {
        stage.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions)
          case None => df
        }
      }
      case partitionBy => {
        // create a column array for repartitioning
        val partitionCols = partitionBy.map(col => df(col))
        stage.numPartitions match {
          case Some(numPartitions) => df.repartition(numPartitions, partitionCols:_*)
          case None => df.repartition(partitionCols:_*)
        }
      }
    }
    if (arcContext.immutableViews) repartitionedDF.createTempView(stage.outputView) else repartitionedDF.createOrReplaceTempView(stage.outputView)

    if (!repartitionedDF.isStreaming) {
      stage.stageDetail.put("inputFiles", java.lang.Integer.valueOf(repartitionedDF.inputFiles.length))
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(repartitionedDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(repartitionedDF.rdd.partitions.length))

      if (stage.persist) {
        repartitionedDF.persist(StorageLevel.MEMORY_AND_DISK_SER)
        stage.stageDetail.put("records", java.lang.Long.valueOf(repartitionedDF.count))
      }
    }

    Option(repartitionedDF)
  }

}

