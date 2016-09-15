/**
	* Created by c308241 on 15/09/16.
	*/
abstract class JdbcDB(url: String, user: String, password: String) {

	import scalikejdbc._

	def driver: String

	def limitRows(n: Int): String

	def fetchSize = 100

	Class.forName(driver)
	ConnectionPool.singleton(url, user, password)

	def getSchema(db: String, table: String): List[Map[String, Any]] = DB.readOnly(session =>
		sql"select column_name, data_type	from INFORMATION_SCHEMA.COLUMNS where table_name = ${db}.${table};"
			.map(_.toMap())).list().apply()

	def take(db: String, table: String)(n: Int): List[Map[String, Any]] =
		DB.readOnly(session =>
			sql"select * from ${db}.${table} ${limitRows(n)}"
				.fetchSize(math.min(n, fetchSize))
				.map(rs => rs.toMap())).list.apply()

	def readAll(db: String, table: String): List[Map[String, Any]] =
		DB.readOnly(session =>
			sql"select * from ${db}.${table}"
				.fetchSize(fetchSize)
				.map(rs => rs.toMap())).list.apply()

	def readAllAsDataFrame(db: String, table: String)

}

