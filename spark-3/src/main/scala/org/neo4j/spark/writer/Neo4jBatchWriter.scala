package org.neo4j.spark.writer

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.connector.write.{BatchWrite, DataWriterFactory, PhysicalWriteInfo, WriterCommitMessage}
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.StructType
import org.neo4j.spark.service.SchemaService
import org.neo4j.spark.util.{DriverCache, Neo4jOptions}

class Neo4jBatchWriter(jobId: String,
                       structType: StructType,
                       saveMode: SaveMode,
                       neo4jOptions: Neo4jOptions) extends BatchWrite with InsertableRelation {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = {
    val schemaService = new SchemaService(neo4jOptions, driverCache)
    schemaService.createOptimizations()
    val scriptResult = schemaService.execute(neo4jOptions.script)
    schemaService.close()

    new Neo4jDataWriterFactory(
      jobId,
      structType,
      saveMode,
      neo4jOptions,
      scriptResult
    )
  }

  private val driverCache = new DriverCache(neo4jOptions.connection, jobId)

  override def commit(messages: Array[WriterCommitMessage]): Unit = {
    driverCache.close()
  }

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    driverCache.close()
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    new Neo4jDataWriter(jobId = jobId, partitionId = -1, schema = data.schema,
      saveMode = if (overwrite) SaveMode.Overwrite else SaveMode.Append,
      options = neo4jOptions,
      scriptResult = java.util.Collections.emptyList()).insert(data)
  }
}