package org.neo4j.spark

import org.junit.Assert.{assertEquals, fail}
import org.junit.Test
import org.neo4j.spark.util.Validations

class VersionValidationTSE extends SparkConnectorScalaBaseTSE {

  @Test
  def testThrowsExceptionSparkVersionIsNotSupported(): Unit = {
    try {
      Validations.version("3.*")
    } catch {
      case e: IllegalArgumentException =>
        assertEquals(
          """Your currentSpark version 2.4.5 is not supported by the current connector.
            |Please visit https://neo4j.com/developer/spark/overview/#_spark_compatibility to know which connector version you need.
            |""".stripMargin, e.getMessage)
      case _: Throwable => fail(s"should be thrown a ${classOf[IllegalArgumentException].getName}")
    }
  }

}
