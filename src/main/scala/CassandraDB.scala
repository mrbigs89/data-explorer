import com.datastax.driver.core.ColumnDefinitions.Definition
import com.datastax.driver.core.{Cluster, Row}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.cassandra._

import collection.JavaConverters._
/**
	* Created by c308241 on 15/09/16.
	*/
class CassandraDB(val hosts: Seq[String], val port: Int) {
	val cluster = Cluster.builder()
		.addContactPoints(hosts.toArray: _*)
		.build()

	lazy val session = cluster.connect()

	lazy val spark = SparkSession.builder()
		.master("local[*]")
		.appName("data-explorer")
		.config("spark.cassandra.connection.host", hosts.mkString(","))
		.getOrCreate()

	/***
		* Returns the schema deducing it from the first row.
		* @param keySpace
		* @param table
		* @return
		*/
	def getSchema(keySpace: String, table: String): Iterable[Definition] = {
		 session.execute(s"select * from $keySpace.$table;").one().getColumnDefinitions.asScala
	}

	/***
		* Selects the first n rows from keySpace.table.
		* @param keySpace
		* @param table
		* @param n
		* @return
		*/
	def take(keySpace: String, table: String)(n: Int): Seq[Row] =
		session.execute(s"select * from $keySpace.$table limit $n;").all().asScala

	def readAsDataFrame(keySpace: String, table: String): DataFrame =
		spark.read.cassandraFormat(table, keySpace).load()

	def query(sql: String): Seq[Row] = session.execute(sql).all().asScala
}

