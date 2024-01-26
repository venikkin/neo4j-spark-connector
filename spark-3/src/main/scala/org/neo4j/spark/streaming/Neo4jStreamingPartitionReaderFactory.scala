package org.neo4j.spark.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.neo4j.spark.service.PartitionPagination
import org.neo4j.spark.util.Neo4jOptions

case class Neo4jStreamingPartition(partitionSkipLimit: PartitionPagination, filters: Array[Filter])
  extends InputPartition

class Neo4jStreamingPartitionReaderFactory(private val neo4jOptions: Neo4jOptions,
                                           private val schema: StructType,
                                           private val jobId: String,
                                           private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
//                                           private val offsetAccumulator: OffsetStorage[java.lang.Long, java.lang.Long],
                                           private val aggregateColumns: Array[AggregateFunc]) extends PartitionReaderFactory {
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = new Neo4jStreamingPartitionReader(
    neo4jOptions,
    partition.asInstanceOf[Neo4jStreamingPartition].filters,
    schema,
    jobId,
    partition.asInstanceOf[Neo4jStreamingPartition].partitionSkipLimit,
    scriptResult,
//    offsetAccumulator,
    new StructType(),
    aggregateColumns
  )
}
