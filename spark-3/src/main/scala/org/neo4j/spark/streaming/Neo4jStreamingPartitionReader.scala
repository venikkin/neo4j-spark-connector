package org.neo4j.spark.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.neo4j.spark.service.PartitionPagination
import org.neo4j.spark.util.Neo4jOptions

class Neo4jStreamingPartitionReader(private val options: Neo4jOptions,
                                    private val filters: Array[Filter],
                                    private val schema: StructType,
                                    private val jobId: String,
                                    private val partitionSkipLimit: PartitionPagination,
                                    private val scriptResult: java.util.List[java.util.Map[String, AnyRef]],
//                                    private val offsetAccumulator: OffsetStorage[java.lang.Long, java.lang.Long],
                                    private val requiredColumns: StructType,
                                    private val aggregateColumns: Array[AggregateFunc])
  extends BaseStreamingPartitionReader(options, filters, schema, jobId, partitionSkipLimit, scriptResult, /*offsetAccumulator,*/ requiredColumns, aggregateColumns)
    with PartitionReader[InternalRow] {
}
