package com.ligadata.filestatvalidator

import java.io._
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Properties

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.security.UserGroupInformation
import org.apache.logging.log4j.LogManager

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object FileStatsValidator {

  lazy val loggerName = this.getClass.getName
  lazy val logger = LogManager.getLogger(loggerName)


  def getConnection3(): Connection = {
    logger.warn("FileStatValidator : Getting jdbc connection to Hive Instance")
    var con: Connection = null
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      var conf: org.apache.hadoop.conf.Configuration = new org.apache.hadoop.conf.Configuration()
      println(">>>>>>>>>>>>>>>>>>>>> 3 " + "jdbc:hive2://jbd1node04.digicelgroup.local:10000/;principal=hive/_HOST@DIGICELGROUP.LOCAL")
      conf.set("hadoop.security.authentication", "Kerberos")
      UserGroupInformation.setConfiguration(conf)
      UserGroupInformation.loginUserFromKeytab("kamanjaprod@DIGICELGROUP.LOCAL", "/home/kamanjaprod/kamanjaprod.keytab")
      con = DriverManager.getConnection("jdbc:hive2://jbd1node04.digicelgroup.local:10000/default;principal=hive/_HOST@DIGICELGROUP.LOCAL", "", "")
      //      System.out.println("got connection")
      logger.warn("FileStatValidator : Connection successful")
    }
    catch {
      case ex: Exception => {
        logger.error(ex)
        System.exit(1)
      }
    }
    return con
  }


  def getConnection2(hiveHost: String, hivePort: String, sslTrustStorePath: String, trustStorePassword: String): Connection = {
    logger.warn("FileStatValidator : Getting jdbc connection to Hive Instance")
    val driverName: String = "org.apache.hive.jdbc.HiveDriver"
    var con: Connection = null
    //jdbc:hive2://<host>:<port>/<db>;ssl=true;sslTrustStore=<trust_store_path>;trustStorePassword=<trust_store_password>
    //    var con: Connection = DriverManager.getConnection("jdbc:hive2://<host>:<port>/default;ssl=true;sslTrustStore=<trust_store_path>;trustStorePassword=<trust_store_password>", "hive", "")
    try {
      Class.forName(driverName)
      println(">>>>>>>>>>>>>>>>>>>>> 1 " + "jdbc:hive2://" + hiveHost + ":" + hivePort + "/default;ssl=true;sslTrustStore=" + sslTrustStorePath + ";trustStorePassword=" + trustStorePassword)
      println(">>>>>>>>>>>>>>>>>>>>> 2 " + "jdbc:hive2://" + hiveHost + ":" + hivePort + "/default")
      //      var con: Connection = DriverManager.getConnection("jdbc:hive2://" + hiveHost + ":" + hivePort + "/default;ssl=true;sslTrustStore=" + sslTrustStorePath + ";trustStorePassword=" + trustStorePassword, "hive", "")
      con = DriverManager.getConnection("jdbc:hive2://" + hiveHost + ":" + hivePort + "/default", "hive", "")

      logger.warn("FileStatValidator : Connection successful")
    } catch {
      case ex: Exception => {
        logger.error(ex)
        System.exit(1)
      }
    }
    return con
  }

  def getConnection(hiveConf: HiveConf): Connection = {
    logger.warn("FileStatValidator : Getting jdbc connection to Hive Instance")
    var con: Connection = null

    try {
      Class.forName(hiveConf.getVar(ConfVars.METASTORE_CONNECTION_DRIVER));
      con = DriverManager.getConnection(
        hiveConf.getVar(ConfVars.METASTORECONNECTURLKEY),
        hiveConf.getVar(ConfVars.METASTORE_CONNECTION_USER_NAME),
        hiveConf.getVar(ConfVars.METASTOREPWD));
    } catch {
      case ex: Exception => {
        logger.error(ex)
        System.exit(1)
      }
    }
    logger.warn("FileStatValidator : Connection successful")
    return con
  }


  //  def validateFileStats(fileStatsTableName: String, successEventsTableName: String, failedEventsTableName: String, partitionFieldName: String, partitionDate: String, conn: Connection): ArrayBuffer[(String, String)] = {
  def validateFileStats(fileStatsTableName: String, fileStatsTablePartitionFiledName: String, fileStatsTablePartitionDate: String, fileStatsTablePartitionStartHour: String, fileStatsTablePartitionEndHour: String, successEventsTablesNames: String, successEventsTablePartitionFiledName: String, successEventsTablePartitionValue: String, failedEventsTableName: String, failedEventsTablePartitionFiledName: String, failedEventsTablePartitionValue: String, feedsToFileNamesMappingLocation: String, conn: Connection): ArrayBuffer[(String, Boolean, String)] = {

    var finalResult: ArrayBuffer[(String, Boolean, String)] = new ArrayBuffer[(String, Boolean, String)]
    var fileNamesAndRecordCounts: ArrayBuffer[(String, Double)] = new ArrayBuffer[(String, Double)]


    //    logger.debug("FileStatValidator : Listing all databases....")
    //    var st5: Statement = conn.prepareStatement("show databases")
    //    val query0: String = "show databases"
    //    try {
    //      st5.execute(query0)
    //      val rs: ResultSet = st5.getGeneratedKeys()
    //      while (rs.next()) {
    //        println(rs.getString(1))
    //      }
    //    } catch {
    //      case e: SQLSyntaxErrorException => {
    //        logger.error("FileStatValidator : error running statement: " + query0, e)
    //      }
    //    }


    logger.debug("FileStatValidator : Getting all unique file names and recordscount for given date partition in table " + fileStatsTableName)
    //Step 1 : get all unique file names and recordscount for given date partition in table ch11_test.file_stats

    //    var whereStatement: String = " where ( " + fileStatsTablePartitionFiledName + "='" + fileStatsTablePartitionDate + "' AND A.recordscount>0 AND `hour` >=" + fileStatsTablePartitionStartHour + " AND `hour` <=" + fileStatsTablePartitionEndHour + ")"
    //    val query1: String = "Select distinct(filename), recordscount from " + fileStatsTableName + whereStatement
    val query1: String = "select count(*) from ch11_test.file_stats"
    println(">>>>>>>>>>>>>>>>>>>>" + query1)
    try {
      val st1: PreparedStatement = conn.prepareStatement(query1)
      //      st1.setDouble(1, 1)
      st1.execute()
      val rs1: ResultSet = st1.getGeneratedKeys

      while (rs1.next()) {
        var fullPath: String = rs1.getString(1)
        var fileName: String = fullPath.substring(fullPath.lastIndexOf("/"), fullPath.length)
        fileNamesAndRecordCounts += ((fileName, rs1.getDouble(2)))
      }
    } catch {
      case e: Exception => {
        logger.error("FileStatValidator : error running statement: " + query1, e)
      }
    }

    logger.debug("FileStatValidator : Getting all file names and recordscount for given date partition in successEventTables : " + successEventsTablesNames)

    //Step 2 : get all unique file names and recordscount for given date partition in all outputTables;
    val successTablesNamesList: Array[String] = successEventsTablesNames.split(",")
    var successEventsFilesAndCounts: mutable.HashMap[String, Double] = new mutable.HashMap[String, Double]

    successTablesNamesList.foreach(successEventTableName => {
      //   select file_name, count(*) from kprod.SandvineReconciliation where date_loaded='2016-12-06' group by file_name
      //      var whereStatement2: String = " where " + successEventsTablePartitionFiledName + "='" + successEventsTablePartitionValue + "'"
      val query2: String = "select file_name, count(*) from ? where ? ='?' group by file_name"

      try {
        val st2: PreparedStatement = conn.prepareStatement(query2)
        st2.setString(1, successEventTableName)
        st2.setString(2, successEventsTablePartitionFiledName)
        st2.setString(3, successEventsTablePartitionValue)
        st2.execute()
        val rs2: ResultSet = st2.getGeneratedKeys
        while (rs2.next()) {
          successEventsFilesAndCounts.put(rs2.getString(1), rs2.getDouble(2))
        }
      } catch {
        case e: Exception => {
          logger.error("FileStatValidator : error running statement: " + query2, e)
        }
      }
    })

    logger.debug("FileStatValidator : Getting all file names and recordscount for given date partition in failedEventsTable : " + failedEventsTableName)
    //Step 3 : get all unique file names and recordscount for given date partition in failedEventsTable;
    var failedEventsFilesAndCounts: mutable.HashMap[String, Double] = new mutable.HashMap[String, Double]

    //    Select filename, count(*) from kprod.rejecteddata where rejectiondate='20161206' group by filename
    //    var whereStatement3: String = " where " + failedEventsTablePartitionFiledName + "='" + failedEventsTablePartitionValue + "'"
    //    val query3: String = "Select filename, count(*) from " + failedEventsTableName + whereStatement3 + " group by filename"
    val query3: String = "select filename, count(*) from ? where ? ='?' group by filename"

    try {
      val st3: PreparedStatement = conn.prepareStatement(query3)
      st3.setString(1, failedEventsTableName)
      st3.setString(2, failedEventsTablePartitionFiledName)
      st3.setString(3, failedEventsTablePartitionValue)
      st3.execute()
      val rs3: ResultSet = st3.getGeneratedKeys
      while (rs3.next()) {
        failedEventsFilesAndCounts.put(rs3.getString(1), rs3.getDouble(2))
      }
    } catch {
      case e: Exception => {
        logger.error("FileStatValidator : error running statement: " + query3, e)
      }
    }

    logger.debug("FileStatValidator : Caluclating stats for each file found in FileStatsTable : " + fileStatsTableName)
    fileNamesAndRecordCounts.foreach(oneFileStats => {
      val sucessRecordsCount: Double = successEventsFilesAndCounts.getOrElse(oneFileStats._1, -1)
      val failedRecordsCount: Double = failedEventsFilesAndCounts.getOrElse(oneFileStats._1, -1)

      var fileStatMatch: Boolean = false
      var failurePercentage: String = "-1"

      if (oneFileStats == (sucessRecordsCount + failedRecordsCount)) {
        fileStatMatch = true
      }

      failurePercentage = calculateFailurePercentage(sucessRecordsCount, failedRecordsCount)

      finalResult += ((oneFileStats._1, fileStatMatch, failurePercentage))
    })


    //    //Step 2 : get a hashmap of FilePath and SuccessTableName mapping
    //    var filePathToTableNameMap: mutable.HashMap[String, String] = jsonToHashMap(feedsToFileNamesMappingLocation)
    //
    //    logger.debug("FileStatValidator : Looping on All files retrieved from " + fileStatsTableName)
    //    fileNamesAndRecordCounts.foreach(oneFileStats => {
    //      val fullFileName = oneFileStats._1
    //      val fileName = fullFileName.substring(fullFileName.lastIndexOf("/"), fullFileName.length)
    //      val filePath = fullFileName.substring(0, fullFileName.lastIndexOf("/"))
    //      val recordsCount = oneFileStats._2
    //
    //      //Step 3 : get correct SuccessTableName
    //      var successEventsTableName: String = ""
    //      breakable {
    //        filePathToTableNameMap.foreach(x => {
    //          if (x._2.contains(filePath)) {
    //            successEventsTableName = x._1
    //            break
    //          }
    //        })
    //      }
    //      logger.debug("FileStatValidator : successEventsTableName is: " + successEventsTableName)
    //
    //      var successEventsCount: Double = -1
    //      var failedEventsCount: Double = -1
    //      var resultCountValidation: String = ""
    //      var failurePercentage: String = ""
    //
    //      logger.debug("FileStatValidator : Getting the number of records for file " + fileName + " from table " + successEventsTableName)
    //      // Step 4 : get the number of records from the SuccessEventsTable
    //      val query2 = "Select count(*) from " + successEventsTableName + " where " + successEventsTablePartitionFiledName + "=" + successEventsTablePartitionValue + " and filename=" + fileName
    //
    //      try {
    //        val rs2: ResultSet = st.executeQuery(query2)
    //        while (rs2.next()) {
    //          successEventsCount = rs2.getDouble(1)
    //        }
    //      } catch {
    //        case e: Exception => {
    //          logger.error("FileStatValidator : error running statement: " + query2, e)
    //        }
    //      }
    //      logger.debug("FileStatValidator : Getting the number of records for file " + fileName + " from table " + failedEventsTableName)
    //      // Step5 : get the number of records from the FailedEventsTable
    //      val query3 = "Select count(*) from " + failedEventsTableName + " where " + failedEventsTablePartitionFiledName + "=" + failedEventsTablePartitionValue + " and filename=" + fileName
    //
    //      try {
    //        val rs3: ResultSet = st.executeQuery(query3)
    //        while (rs3.next()) {
    //          failedEventsCount = rs3.getDouble(1)
    //        }
    //      } catch {
    //        case e: Exception => {
    //          logger.error("FileStatValidator : error running statement: " + query3, e)
    //        }
    //      }
    //      logger.debug("FileStatValidator : Validating records count")
    //      if (recordsCount == (successEventsCount + failedEventsCount)) {
    //        resultCountValidation = "count matches for file " + fileName
    //      } else {
    //        resultCountValidation = "count Does NOT matche for file " + fileName + ", file_stats count : %1.0f , SuccessEventsTable count : %1.0f , FailedEventsTable count : %1.2f , difference is : %1.0f".format(
    //          recordsCount, successEventsCount, failedEventsCount, recordsCount - successEventsCount - failedEventsCount)
    //      }
    //
    //      logger.debug("FileStatValidator : Calculating failure percentage")
    //      failurePercentage = calculateFailurePercentage(successEventsCount, failedEventsCount)
    //      finalResult += ((resultCountValidation, failurePercentage))
    //
    //    })
    logger.debug("FileStatValidator : Returning FinalResult....")
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


  //  def jsonToHashMap(pathToJsonFile: String): mutable.HashMap[String, String] = {
  //    var hashmap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]
  //    var bufferedReader: BufferedReader = null
  //    var jsonObj: JSONObject = null
  //    var keysList: List[String] = null
  //    try {
  //      var encoded: Array[Byte] = Files.readAllBytes(Paths.get(pathToJsonFile))
  //      var jsonString: String = new String(encoded, "UTF-8")
  //      jsonObj = new JSONObject(jsonString)
  //
  //      var keysToCopyIterator: util.Iterator[_] = jsonObj.keys()
  //
  //      while (keysToCopyIterator.hasNext) {
  //        var oneKey: String = String.valueOf(keysToCopyIterator.next())
  //        var value: String = String.valueOf(jsonObj.get(oneKey))
  //        hashmap += (oneKey -> value)
  //
  //      }
  //    }
  //
  //    return hashmap
  //  }


  def main(args: Array[String]): Unit = {

    val hiveSiteXmlPath = args(0)
    //example:  file:///path/to/hive-site.xml
    val propertiesFilePath = args(1)

    var hiveHost: String = ""
    var hivePort: String = ""
    var sslTrustStorePath: String = ""
    var trustStorePassword: String = ""


    var fileStatsTableName: String = ""
    var fileStatsTablePartitionFiledName: String = ""
    var fileStatsTablePartitionDate: String = ""
    var fileStatsTablePartitionStartHour: String = ""
    var fileStatsTablePartitionEndHour: String = ""

    var successEventsTablesNames: String = ""
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
      logger.debug("FileStatValidator : Capturing properties from properties file : " + propertiesFilePath)
      input = new FileInputStream(propertiesFilePath)
      // load a properties file
      prop.load(input)
      // get the property value and print it out
      //      hiveHost: String, hivePort: String, sslTrustStorePath: String, trustStorePassword: String

      hiveHost = prop.getProperty("hive.host")
      hivePort = prop.getProperty("hive.port")
      sslTrustStorePath = prop.getProperty("ssl.trust.store.path")
      trustStorePassword = prop.getProperty("trust.store.password")

      successEventsTablesNames = prop.getProperty("success.events.tables.names")
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

      println("hiveHost: " + hiveHost)
      println("hivePort: " + hivePort)
      println("sslTrustStorePath: " + sslTrustStorePath)
      println("trustStorePassword: " + trustStorePassword)
      println("fileStatsTableName: " + fileStatsTableName)
      println("fileStatsTablePartitionFiledName: " + fileStatsTablePartitionFiledName)
      println("fileStatsTablePartitionDate: " + fileStatsTablePartitionDate)
      println("fileStatsTablePartitionStartHour: " + fileStatsTablePartitionStartHour)
      println("fileStatsTablePartitionEndHour: " + fileStatsTablePartitionEndHour)
      println("successEventsTablesNames: " + successEventsTablesNames)
      println("successEventsTablePartitionValue: " + successEventsTablePartitionValue)
      println("successEventsTablePartitionFiledName: " + successEventsTablePartitionFiledName)
      println("failedEventsTableName: " + failedEventsTableName)
      println("failedEventsTablePartitionFiledName: " + failedEventsTablePartitionFiledName)
      println("failedEventsTablePartitionValue: " + failedEventsTablePartitionValue)
      println("feedsToFileNamesMappingLocation: " + feedsToFileNamesMappingLocation)

      /////////////////////////////////////////////////////////////


      //      connection = getConnection(hiveConf)
      //      connection = getConnection2(hiveHost, hivePort, sslTrustStorePath, trustStorePassword)
      connection = getConnection3()
      val result1 = validateFileStats(fileStatsTableName, fileStatsTablePartitionFiledName, fileStatsTablePartitionDate, fileStatsTablePartitionStartHour, fileStatsTablePartitionEndHour, successEventsTablesNames, successEventsTablePartitionFiledName, successEventsTablePartitionValue, failedEventsTableName, failedEventsTablePartitionFiledName, failedEventsTablePartitionValue, feedsToFileNamesMappingLocation, connection)


      logger.debug("FileStatValidator : Printing out FinalResult....")
      result1.foreach(singleFileResult => {
        println("FileStatsValidation Result : (" + singleFileResult._1 + ", " + singleFileResult._2 + ", " + singleFileResult._3 + ")")
      })


      logger.debug("FileStatValidator : Done....")

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
