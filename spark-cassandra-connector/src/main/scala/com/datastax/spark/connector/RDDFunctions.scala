package com.datastax.spark.connector

import java.net.InetAddress

import com.datastax.spark.connector.cql.{CassandraConnector}
import com.datastax.spark.connector.rdd.{SpannedRDD}
import com.datastax.spark.connector.writer._
import com.datastax.spark.connector.writer.ReplicaMapper
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Partitioner

import scala.reflect.ClassTag

import scala.util.Random


/** Provides Cassandra-specific methods on `RDD` */
class RDDFunctions[T](rdd: RDD[T]) extends WritableToCassandra[T] with Serializable {

  override val sparkContext: SparkContext = rdd.sparkContext

  /**
   * Saves the data from `RDD` to a Cassandra table. Uses the specified column names.
   * @see [[com.datastax.spark.connector.writer.WritableToCassandra]]
   */
  def saveToCassandra(keyspaceName: String,
                      tableName: String,
                      columns: ColumnSelector = AllColumns,
                      writeConf: WriteConf = WriteConf.fromSparkConf(sparkContext.getConf))
                     (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                      rwf: RowWriterFactory[T]): Unit = {
    val writer = TableWriter(connector, keyspaceName, tableName, columns, writeConf)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  /** Applies a function to each item, and groups consecutive items having the same value together.
    * Contrary to `groupBy`, items from the same group must be already next to each other in the
    * original collection. Works locally on each partition, so items from different
    * partitions will never be placed in the same group.*/
  def spanBy[U](f: (T) => U): RDD[(U, Iterable[T])] =
    new SpannedRDD[U, T](rdd, f)

  def keyByReplica(keyspaceName: String, tableName: String)
                           (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                            rwf: RowWriterFactory[T]): RDD[(Set[InetAddress],T)] = {
      val converter = ReplicaMapper(connector, keyspaceName,tableName)
      rdd.mapPartitions( primaryKeys =>
        converter.mapReplicas(primaryKeys)
      )
  }

  def partitionByReplica(keyspaceName: String, tableName: String, partitionsPerHost: Int = 10)
                  (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                   rwf: RowWriterFactory[T], tag: ClassTag[T] ): RDD[T] = {

      class ReplicaPartitioner(partitionsPerHost:Int) extends Partitioner{
        val hosts = connector.hosts
        val host_map = hosts.zipWithIndex.toMap
        val num_hosts = hosts.size
        val rand = new Random()
        override def getPartition(key:Any): Int = {
              val replicaSet = key.asInstanceOf[Set[InetAddress]]
              val offset = rand.nextInt(partitionsPerHost)
              host_map.getOrElse(replicaSet.last, rand.nextInt(num_hosts)) + offset
          }
        override def numPartitions: Int = partitionsPerHost * num_hosts
    }

    rdd.keyByReplica(keyspaceName, tableName)
      .partitionBy(new ReplicaPartitioner(partitionsPerHost))
      .map(_._2)
  }

  /**
  def fetchFromCassandra[U](keyspaceName: String, tableName: String)
                           (implicit connector: CassandraConnector = CassandraConnector(sparkContext.getConf),
                            ct: ClassTag[U], rrf: RowReaderFactory[U],
                            ev: ValidRDDType[U], rwf: RowWriterFactory[T]): CassandraRDD[U] = {
    rdd.foreachPartition{ primaryKeys => connector.withSessionDo()
    }
  } **/


}
