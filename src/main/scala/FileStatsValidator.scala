import java.io.{FileInputStream, IOException, InputStream}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.logging.log4j.LogManager

import scala.collection.mutable.ArrayBuffer

/**
  * Created by haitham.b on 12/1/2016.
  */
class FileStatsValidator {

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)


  def getConnection(hiveConf: HiveConf): Connection = {

    var conn: Connection = null

    try {
      Class.forName(hiveConf.getVar(ConfVars.METASTORE_CONNECTION_DRIVER));
      conn = DriverManager.getConnection(
        hiveConf.getVar(ConfVars.METASTORECONNECTURLKEY),
        hiveConf.getVar(ConfVars.METASTORE_CONNECTION_USER_NAME),
        hiveConf.getVar(ConfVars.METASTOREPWD));
    }

    return conn
  }


  def validateFileStats(fileStatsTableName: String, successEventsTableName: String, failedEventsTableName: String, partitionFieldName: String, partitionDate: String, conn: Connection): ArrayBuffer[(String, String)] = {

    var finalResult: ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]
    var fileNamesAndRecordCounts: ArrayBuffer[(String, Double)] = new ArrayBuffer[(String, Double)]

    //Step 1 : get all unique file names and recordscount for given date partition in table ch11_test.file_stats
    var st: Statement = conn.createStatement()
    val query1: String = " Select distinct(filename), recordscount from " + fileStatsTableName + " where " + partitionFieldName + "=" + partitionDate
    val rs1: ResultSet = st.executeQuery(query1)

    while (rs1.next()) {
      fileNamesAndRecordCounts += ((rs1.getString(1), rs1.getDouble(2)))
    }


    fileNamesAndRecordCounts.foreach(oneFileStats => {
      val fullFileName = oneFileStats._1
      val fileName = fullFileName.substring(fullFileName.lastIndexOf("/"), fullFileName.length)
      val recordsCount = oneFileStats._2

      var successEventsCount: Double = -1
      var failedEventsCount: Double = -1
      var resultCountValidation: String = ""
      var failurePercentage: String = ""

      // Step 2 : get the number of records from the SuccessEventsTable
      val query2 = "Select count(*) from " + successEventsTableName + " where " + partitionFieldName + "=" + partitionDate + " and filename=" + fileName
      val rs2: ResultSet = st.executeQuery(query2)
      while (rs2.next()) {
        successEventsCount = rs2.getDouble(1)
      }

      // Step3 : get the number of records from the FailedEventsTable
      val query3 = "Select count(*) from " + failedEventsTableName + " where " + partitionFieldName + "=" + partitionDate + " and filename=" + fileName
      val rs3: ResultSet = st.executeQuery(query3)
      while (rs3.next()) {
        failedEventsCount = rs3.getDouble(1)
      }

      if (recordsCount == (successEventsCount + failedEventsCount)) {
        resultCountValidation = "count matches for file " + fileName
      } else {
        resultCountValidation = "count Does NOT matche for file " + fileName + ", file_stats count : %1.0f , SuccessEventsTable count : %1.0f , FailedEventsTable count : %1.2f , difference is : %1.0f".format(
          recordsCount, successEventsCount, failedEventsCount, recordsCount - successEventsCount - failedEventsCount)
      }

      failurePercentage = calculateFailurePercentage(successEventsCount, failedEventsCount)
      finalResult += ((resultCountValidation, failurePercentage))

    })

    finalResult
  }

  def calculateFailurePercentage(successEventsCount: Double, failedEventsCount: Double): String = {
    var failurePercentage: Double = -1
    if (successEventsCount != 0) {
      failurePercentage = 100 * (failedEventsCount / successEventsCount)
    }

    return "%1.2f" format failurePercentage
  }


  def main(args: Array[String]): Unit = {

    val hiveSiteXmlPath = args(0)
    //example:  file:///path/to/hive-site.xml
    val propertiesFilePath = args(1)

    var fileStatsTableName: String = ""
    var successEventsTableName: String = ""
    var failedEventsTableName: String = ""
    var partitionDate: String = ""
    var partitionFieldName: String = ""
    var hiveInstanceLocationAndPort: String = ""


    var connection: Connection = null
    var hiveConf: HiveConf = new HiveConf()
    hiveConf.addResource(new Path(hiveSiteXmlPath))

    var prop: Properties = new Properties()
    var input: InputStream = null

    try {
      input = new FileInputStream(propertiesFilePath)
      // load a properties file
      prop.load(input)
      // get the property value and print it out
      fileStatsTableName = prop.getProperty("file.stats.table.name")
      successEventsTableName = prop.getProperty("success.events.table.name")
      failedEventsTableName = prop.getProperty("failed.events.table.name")
      partitionDate = prop.getProperty("partition.date")
      partitionFieldName = prop.getProperty("partition.field.name")

      connection = getConnection(hiveConf)
      val result1 = validateFileStats(fileStatsTableName, successEventsTableName, failedEventsTableName, partitionFieldName, partitionDate, connection)

      result1.foreach(singleFileResult => {
        println("Records Count Validation Result: " + singleFileResult._1)
        println("Failure Percentage Result : " + singleFileResult._2)
      })


    } catch {
      case ex: IOException => {
        ex.printStackTrace()
      }
    } finally {
      connection.close()
      if (input != null) {
        try {
          input.close();
        } catch {
          case e: IOException => {
            e.printStackTrace()
          }
        }
      }
    }

  }

}
