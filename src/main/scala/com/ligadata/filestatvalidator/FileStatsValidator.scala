package com.ligadata.filestatvalidator

import java.io._
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.logging.log4j.LogManager
import org.json.JSONObject

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object FileStatsValidator {

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)


  def getConnection(hiveConf: HiveConf): Connection = {
    logger.warn("FileStatValidator : Getting jdbc connection to Hive Instance")
    var conn: Connection = null

    try {
      Class.forName(hiveConf.getVar(ConfVars.METASTORE_CONNECTION_DRIVER));
      conn = DriverManager.getConnection(
        hiveConf.getVar(ConfVars.METASTORECONNECTURLKEY),
        hiveConf.getVar(ConfVars.METASTORE_CONNECTION_USER_NAME),
        hiveConf.getVar(ConfVars.METASTOREPWD));
    } catch {
      case ex: Exception => {
        logger.error(ex)
      }
    }
    logger.warn("FileStatValidator : Connection successful")
    println("FileStatValidator : Connection successful")
    return conn
  }


  //  def validateFileStats(fileStatsTableName: String, successEventsTableName: String, failedEventsTableName: String, partitionFieldName: String, partitionDate: String, conn: Connection): ArrayBuffer[(String, String)] = {
  def validateFileStats(fileStatsTableName: String, fileStatsTablePartitionFiledName: String, fileStatsTablePartitionDate: String, fileStatsTablePartitionStartHour: String, fileStatsTablePartitionEndHour: String, successEventsTablePartitionFiledName: String, successEventsTablePartitionValue: String, failedEventsTableName: String, failedEventsTablePartitionFiledName: String, failedEventsTablePartitionValue: String, feedsToFileNamesMappingLocation: String, conn: Connection): ArrayBuffer[(String, String)] = {

    var finalResult: ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]
    var fileNamesAndRecordCounts: ArrayBuffer[(String, Double)] = new ArrayBuffer[(String, Double)]

    logger.debug("FileStatValidator : Getting all unique file names and recordscount for given date partition in table " + fileStatsTableName)

    //Step 1 : get all unique file names and recordscount for given date partition in table ch11_test.file_stats
    var st: Statement = conn.createStatement()
    var whereStatement: String = " where " + fileStatsTablePartitionFiledName + "='" + fileStatsTablePartitionDate + "' AND recordscount!=0" + " AND hour>=" + fileStatsTablePartitionStartHour + " AND hour<=" + fileStatsTablePartitionEndHour
    val query1: String = "Select distinct(filename), recordscount from " + fileStatsTableName + whereStatement + " limit 20"
    val rs1: ResultSet = st.executeQuery(query1)

    while (rs1.next()) {
      fileNamesAndRecordCounts += ((rs1.getString(1), rs1.getDouble(2)))
    }


    //Step 2 : get a hashmap of FilePath and SuccessTableName mapping
    var filePathToTableNameMap: mutable.HashMap[String, String] = jsonToHashMap(feedsToFileNamesMappingLocation)

    logger.debug("FileStatValidator : Looping on All files retrieved from " + fileStatsTableName)
    fileNamesAndRecordCounts.foreach(oneFileStats => {
      val fullFileName = oneFileStats._1
      val fileName = fullFileName.substring(fullFileName.lastIndexOf("/"), fullFileName.length)
      val filePath = fullFileName.substring(0, fullFileName.lastIndexOf("/"))
      val recordsCount = oneFileStats._2

      //Step 3 : get correct SuccessTableName
      var successEventsTableName: String = ""
      breakable {
        filePathToTableNameMap.foreach(x => {
          if (x._2.contains(filePath)) {
            successEventsTableName = x._1
            break
          }
        })
      }
      logger.debug("FileStatValidator : successEventsTableName is: " + successEventsTableName)

      var successEventsCount: Double = -1
      var failedEventsCount: Double = -1
      var resultCountValidation: String = ""
      var failurePercentage: String = ""

      logger.debug("FileStatValidator : Getting the number of records for file " + fileName + " from table " + successEventsTableName)
      // Step 4 : get the number of records from the SuccessEventsTable
      val query2 = "Select count(*) from " + successEventsTableName + " where " + successEventsTablePartitionFiledName + "=" + successEventsTablePartitionValue + " and filename=" + fileName
      val rs2: ResultSet = st.executeQuery(query2)
      while (rs2.next()) {
        successEventsCount = rs2.getDouble(1)
      }

      logger.debug("FileStatValidator : Getting the number of records for file " + fileName + " from table " + failedEventsTableName)
      // Step5 : get the number of records from the FailedEventsTable
      val query3 = "Select count(*) from " + failedEventsTableName + " where " + failedEventsTablePartitionFiledName + "=" + failedEventsTablePartitionValue + " and filename=" + fileName
      val rs3: ResultSet = st.executeQuery(query3)
      while (rs3.next()) {
        failedEventsCount = rs3.getDouble(1)
      }
      logger.debug("FileStatValidator : Validating records count")
      if (recordsCount == (successEventsCount + failedEventsCount)) {
        resultCountValidation = "count matches for file " + fileName
      } else {
        resultCountValidation = "count Does NOT matche for file " + fileName + ", file_stats count : %1.0f , SuccessEventsTable count : %1.0f , FailedEventsTable count : %1.2f , difference is : %1.0f".format(
          recordsCount, successEventsCount, failedEventsCount, recordsCount - successEventsCount - failedEventsCount)
      }

      logger.debug("FileStatValidator : Calculating failure percentage")
      failurePercentage = calculateFailurePercentage(successEventsCount, failedEventsCount)
      finalResult += ((resultCountValidation, failurePercentage))

    })

    finalResult
  }

  def calculateFailurePercentage(successEventsCount: Double, failedEventsCount: Double): String = {
    var failurePercentage: Double = -1
    if (successEventsCount + failedEventsCount > 0) {
      failurePercentage = 100 * (failedEventsCount / (successEventsCount + failedEventsCount))
    } else {
      logger.error("FileStatValidator: successEventsCount + failedEventsCount =" + successEventsCount + failedEventsCount)
    }

    return "%1.2f" format failurePercentage
  }


  def jsonToHashMap(pathToJsonFile: String): mutable.HashMap[String, String] = {
    var hashmap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
    var bufferedReader: BufferedReader = null
    var jsonObj: JSONObject = null
    var keysList: List[String] = null
    try {
      var encoded: Array[Byte] = Files.readAllBytes(Paths.get(pathToJsonFile))
      var jsonString: String = new String(encoded, "UTF-8")
      jsonObj = new JSONObject(jsonString)

      var keysToCopyIterator: util.Iterator[_] = jsonObj.keys()

      while (keysToCopyIterator.hasNext) {
        var oneKey: String = String.valueOf(keysToCopyIterator.next())
        var value: String = String.valueOf(jsonObj.get(oneKey))
        hashmap += (oneKey -> value)

      }
    }

    return hashmap
  }


  def main(args: Array[String]): Unit = {

    val hiveSiteXmlPath = args(0)
    //example:  file:///path/to/hive-site.xml
    val propertiesFilePath = args(1)

    //    var partitionDate: String = ""
    //    var partitionFieldName: String = ""
    //    var hiveInstanceLocationAndPort: String = ""
    var fileStatsTableName: String = ""
    var fileStatsTablePartitionFiledName: String = ""
    var fileStatsTablePartitionDate: String = ""
    var fileStatsTablePartitionStartHour: String = ""
    var fileStatsTablePartitionEndHour: String = ""

    var successEventsTablePartitionValue: String = ""
    var successEventsTablePartitionFiledName: String = ""

    var failedEventsTableName: String = ""
    var failedEventsTablePartitionFiledName: String = ""
    var failedEventsTablePartitionValue: String = ""

    var feedsToFileNamesMappingLocation: String = ""


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
      successEventsTablePartitionValue = prop.getProperty("success.events.table.partition.value")
      successEventsTablePartitionFiledName = prop.getProperty("success.events.table.partition.field.name")

      failedEventsTableName = prop.getProperty("failed.events.table.name")
      failedEventsTablePartitionValue = prop.getProperty("success.events.table.partition.value")
      failedEventsTablePartitionFiledName = prop.getProperty("success.events.table.partition.field.name")

      fileStatsTableName = prop.getProperty("file.stats.table.name")
      fileStatsTablePartitionFiledName = prop.getProperty("file.stats.table.partition.filed.name")
      fileStatsTablePartitionDate = prop.getProperty("file.stats.table.partition.date")
      fileStatsTablePartitionStartHour = prop.getProperty("file.stats.table.partition.start.hour")
      fileStatsTablePartitionEndHour = prop.getProperty("file.stats.table.partition.end.hour")

      feedsToFileNamesMappingLocation = prop.getProperty("feeds.to.filenames.mapping.location")

      //////////////////////////////////////////////////////////

      println("fileStatsTableName: " + fileStatsTableName)
      println("fileStatsTablePartitionFiledName: " + fileStatsTablePartitionFiledName)
      println("fileStatsTablePartitionDate: " + fileStatsTablePartitionDate)
      println("fileStatsTablePartitionStartHour: " + fileStatsTablePartitionStartHour)
      println("fileStatsTablePartitionEndHour: " + fileStatsTablePartitionEndHour)
      println("successEventsTablePartitionValue: " + successEventsTablePartitionValue)
      println("successEventsTablePartitionFiledName: " + successEventsTablePartitionFiledName)
      println("failedEventsTableName: " + failedEventsTableName)
      println("failedEventsTablePartitionFiledName: " + failedEventsTablePartitionFiledName)
      println("failedEventsTablePartitionValue: " + failedEventsTablePartitionValue)
      println("feedsToFileNamesMappingLocation: " + feedsToFileNamesMappingLocation)

      /////////////////////////////////////////////////////////////


      connection = getConnection(hiveConf)
      val result1 = validateFileStats(fileStatsTableName, fileStatsTablePartitionFiledName, fileStatsTablePartitionDate, fileStatsTablePartitionStartHour, fileStatsTablePartitionEndHour, successEventsTablePartitionFiledName, successEventsTablePartitionValue, failedEventsTableName, failedEventsTablePartitionFiledName, failedEventsTablePartitionValue, feedsToFileNamesMappingLocation, connection)

      result1.foreach(singleFileResult => {
        println("Records Count Validation Result: " + singleFileResult._1)
        println("Failure Percentage Result : " + singleFileResult._2)
      })


    } catch {
      case ex: IOException => {
        logger.error(ex)
      }
    } finally {
      connection.close()
      if (input != null) {
        try {
          input.close()
        } catch {
          case e: IOException => {
            logger.error(e)
          }
        }
      }
    }

  }

}
