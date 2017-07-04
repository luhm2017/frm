package com.lakala.finance.anti_fraud.toolbox

import java.io._
import java.sql.{DriverManager, PreparedStatement, ResultSet}
import java.util
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Callable, ExecutorService, Executors, Future}
import javax.management.modelmbean.XMLParseException

import com.alibaba.fastjson.{JSON, JSONException, JSONObject}
import com.lakala.finance.common.CommonUtil

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.xml.{Elem, Node, XML}
import org.apache.log4j.{Logger, PropertyConfigurator}


object Neo4j2Hive {
  //加载jdbc的驱动
  classOf[com.mysql.jdbc.Driver]

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  val outputFileNum = new AtomicInteger(0)

  def main(args: Array[String]): Unit = {
    val conf_path = args(0)
    //手动加载配置文件路径，注意要将jar包和配置文件放在同一个路径
    PropertyConfigurator.configure(s"${conf_path}/log4j.properties");

    //加载日志路径
    logger.warn(s"导出程序开启")
    val start: Long = System.currentTimeMillis()
    val conf: Config = new Config(s"${conf_path}/conf.xml")
    submitExportTasks(conf)
    logger.warn(s"导出完成，程序总共耗时${(System.currentTimeMillis() - start) / 1000}s")

  }

  private def submitExportTasks(conf: Config): Unit = {
    val tasks: util.ArrayList[Callable[Int]] = new util.ArrayList(conf._thread_num)
    for (i <- 0 until conf._thread_num) {
      tasks.add(new Query2CvsTask(conf))
    }
    val pool: ExecutorService = Executors.newFixedThreadPool(conf._thread_num)
    val futures: util.List[Future[Int]] = pool.invokeAll(tasks)
    for (i <- 0 until futures.size()) {
      futures.get(i).get() //阻塞等待任务完成
    }
    pool.shutdown()
  }

  class Query2CvsTask(config: Config) extends Callable[Int] {
    private var taskStopFlag = false

    private var neo4jConn = DriverManager.getConnection(config._url, config._user, config._passwd)

    private val conRest: Boolean = config._sql._where_conf.getOrElse("conRest", "false").toBoolean
    private val conRestNumb = config._sql._where_conf.getOrElse("conRestNumb", "0").toInt
    private val conRestWait = config._sql._where_conf.getOrElse("conRestWait", "0").toInt * 1000

    private val list_batch_num: Int = config._sql._list_conf.getOrElse("batch_numb", Int.MaxValue.toString).toInt

    private val page_on: Boolean = config._sql._paging_conf.getOrElse("on", "false").toBoolean
    private val page_size: Int = if (page_on) config._sql._paging_conf.getOrElse("size", "100000").toInt else 0

    private val sql_type = config._sql._sql_type

    private var sql: String = _

    private var file_size_now: Int = 0
    private var file_name_now: String = _
    private var csv_file_stream: BufferedOutputStream = _

    private var subList: ArrayBuffer[String] = _

    override def call(): Int = {
      try {
        var con_query_num = 0
        while (!taskStopFlag) {
          con_query_num += 1
          if (conRest && con_query_num >= conRestNumb) {
            //根据配置来确定重启连接
            CommonUtil.closeIO(neo4jConn)
            Thread.sleep(conRestWait)
            neo4jConn = DriverManager.getConnection(config._url, config._user, config._passwd)
            logger.warn(s"${Thread.currentThread().getName}的neo4j连接执行了${conRestNumb}个sql，等待${conRestWait},重新开关连接")
          }

          if ("static".equals(sql_type)) {
            if (sql == null)
              sql = mkSql4Query().get
          } else {
            val sql_Op = mkSql4Query()
            sql_Op match {
              case Some(sql_str) => sql = sql_str
              case None => return 0 //表示已经无法构建sql，可以关闭该线程
            }
          }
          logger.debug(s"${Thread.currentThread().getName}执行的sql为：${sql}")
          val ps: PreparedStatement = neo4jConn.prepareStatement(sql)
          if (page_on) {
            //开启分页的场景
            var page_start = 0
            var page_query_Stop = false
            while (!page_query_Stop) {
              ps.setInt(1, page_start)
              ps.setInt(2, page_size)
              page_start += page_size
              val queryStart: Long = System.currentTimeMillis()
              val rs: ResultSet = ps.executeQuery()
              val resultCount: Int = wirteRs2Csv(rs, config._sql._return_metaData, System.currentTimeMillis() - queryStart, page_start - page_size)
              if (resultCount < page_size) {
                //如果这次查询少于分页数量，说明已经查询完毕，不用再次查询
                page_query_Stop = true
              }
            }

          } else {
            //没有开启分页的查询场景
            val queryStart: Long = System.currentTimeMillis()
            val rs: ResultSet = ps.executeQuery()
            wirteRs2Csv(rs, config._sql._return_metaData, System.currentTimeMillis() - queryStart, 0)
          }

          if ("static".equals(sql_type)) {
            taskStopFlag = true
          }
        }
        logger.info(s"${Thread.currentThread().getName}循环查询结束，跳出循环开始准备回收资源")
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
      } finally {
        if(csv_file_stream != null){
          logger.info(s"${Thread.currentThread().getName}查询完毕，关闭流以及neo4j连接")
          csv_file_stream.flush()
          csv_file_stream.close()
        }
        CommonUtil.closeIO(neo4jConn)
      }
      0
    }

    //根据元数据将相应的查询结果写入到csv文件中
    private def wirteRs2Csv(rs: ResultSet, metaData: mutable.LinkedHashMap[String, ArrayBuffer[String]], ioTime: Long, pageStart: Int): Int = {
      var count = 0
      while (rs.next()) {
        var json_str = ""
        try {
          count += 1
          val result: ArrayBuffer[String] = ArrayBuffer()
          metaData.foreach { case (key, fileds) =>
            if (fileds == null) {
              result.+=(rs.getString(key))
            } else {
              json_str = rs.getString(key)
              val json: JSONObject = JSON.parseObject(json_str)
              fileds.foreach(filed => {
                result.+=(json.getString(filed))
              })
            }
          }

          var text: String = ""
          result.foreach(str => {
            if ("".equals(text)) {
              text = str
            } else {
              text = s"${text},${str}"
            }
          })
          text += "\n"
          val bytesOutPut: Array[Byte] = text.getBytes
          if (csv_file_stream == null) {
            file_name_now = s"${config._fileDir}/${outputFileNum.addAndGet(1)}.csv"
            csv_file_stream = new BufferedOutputStream(new FileOutputStream(file_name_now))
            logger.debug(s"${Thread.currentThread().getName}开始写文件:${file_name_now}")
          }

          csv_file_stream.write(bytesOutPut)
          file_size_now += bytesOutPut.length

          if (file_size_now >= config._exportSize) {
            csv_file_stream.flush()
            CommonUtil.closeIO(csv_file_stream)

            file_size_now = 0
            file_name_now = s"${config._fileDir}/${outputFileNum.addAndGet(1)}.csv"
            csv_file_stream = new BufferedOutputStream(new FileOutputStream(file_name_now))
            logger.debug(s"${Thread.currentThread().getName}，开始写${file_name_now}")
          }
        } catch {
          case ex: JSONException =>
            val info: String = CommonUtil.getExceptionInfo(ex)
            logger.error(ex)
            logger.error(s"错误的json为:${json_str}")

        }
      }
      logger.info(s"${Thread.currentThread().getName},io:${ioTime / 1000}s,查询总数：${count},分页起点:${pageStart},分页大小:${page_size},对应的subList：${subList.mkString(",")}")
      count
    }

    private def mkSql4Query(): Option[String] = {
      var sql = mkMatchSql()
      if (config._sql._where_exist) {
        //如果存在where语句，则需要根据配置构建where 语句
        val whereSql: Option[String] = mkWhereSql()
        whereSql match {
          case Some(where_str) => sql = s"${sql} ${where_str}"
          case None => return None
        }
      }
      sql = s"${sql} ${mkReturnSql()}"

      if (page_on) {
        //如果开启了分页则需要设置相应的参数输入项
        sql = s"${sql} skip ? limit ?"
      }

      Some(sql)
    }

    //基于配置信息构建match语句
    private def mkMatchSql(): String = {
      //todo 先实现最基础的match sql，后期添加根据配置动态生成批次计算的sql
      s"MATCH ${config._sql._match_str}"
    }

    private def mkWhereSql(): Option[String] = {
      var where_sql_str = config._sql._where_str
      //基于配置信息生成相应的where语句
      if ("batch".equals(config._sql._where_conf.getOrElse("type", null))) {
        //针对批量的情况需要不同的处理逻辑
        val list = getSubOfList4Batch()
        //记录subList，用于日志打印
        subList = list
        if (list.size == 0) {
          //字符串为空，则表示已经从外部的list完全的获取了数据
          return None
        }
        logger.debug(s"${Thread.currentThread().getName}，开始处理:${list.mkString(",")}")
        val link: String = config._sql._where_conf.getOrElse("link", null)
        if (CommonUtil.isNullOrEmpty(link)) {
          //没有连接符的场景
          where_sql_str = s"WHERE ${where_sql_str}"
          var list_str = ""
          list.foreach(str => {
            list_str = s"${list_str}'${str}',"
          })
          list_str = list_str.substring(0, list_str.length - 1)
          return Some(where_sql_str.replace("$list", list_str))
        } else {
          //有连接符的情况
          val priffix = where_sql_str.replace("$list", "")
          var where_sql = ""
          list.foreach(str => {
            where_sql = s"${where_sql} ${priffix}'${str}'${link}"
          })
          where_sql = where_sql.substring(0, where_sql.length - link.length)
          where_sql = s"WHERE ${where_sql}"
          return Some(where_sql)
        }
      }
      Some(s"WHERE ${where_sql_str}")
    }

    private def mkReturnSql(): String = {
      config._sql._return_str
    }


    private def getSubOfList4Batch(): ArrayBuffer[String] = {
      val list_file: BufferedReader = config._sql._list_file_reader
      var result = ArrayBuffer[String]()
      if ("file".equals(config._sql._list_conf.getOrElse("type", null))) {
        synchronized {
          for (i <- 0 until list_batch_num if !config._sql._list_file_isClose) {
            val orderno: String = list_file.readLine()
            if (!CommonUtil.isNullOrEmpty(orderno)) {
              result.+=(orderno)
            } else {
              config._sql._list_file_isClose = true
              CommonUtil.closeIO(list_file)
            }
          }
        }
        if (result.size < list_batch_num) {
          config._sql._list_file_isClose = true
          CommonUtil.closeIO(list_file)
        }
      }
      result
    }

  }

  //相应的配置信息，从xml中加载到的
  class Config(xmlPath: String) {
    private val confXml: Elem = XML.load(xmlPath)

    val _url = s"jdbc:neo4j:${(confXml \ "neo4j" \ "connect" \ "url").text}"
    val _user = (confXml \ "neo4j" \ "connect" \ "user").text
    val _passwd = (confXml \ "neo4j" \ "connect" \ "passwd").text

    val _sql: QuerySQL = new QuerySQL
    readSqlFromXml(_sql, confXml)

    val _thread_num = (confXml \ "perform" \ "thread_num") (0).text.toInt

    private val _export_conf: Map[String, String] = (confXml \ "export") (0).attributes.asAttrMap
    val _exportType: String = _export_conf.getOrElse("type", "").toString
    val _exportSize: Int = _export_conf.getOrElse("size", "500").toInt * 1024 * 1024
    val _fileDir: String =
      if ("file".equals(_exportType)) {
        val dirpath: String = _export_conf.getOrElse("fileDir", "").toString
        val file: File = new File(dirpath)
        if (!file.mkdir()) {
          throw new IOException(s"目录${dirpath}已经存在，请配置正确的路径")
        }
        logger.info(s"成功创建文件目录${dirpath}")
        dirpath
      }
      else null


    //todo 后期随着业务添加，可能需要修改此处的逻辑，以及相应的sql类
    private def readSqlFromXml(sql: QuerySQL, confXml: Elem) = {
      sql._sql_type = (confXml \ "neo4j" \ "sql") (0).attribute("type").get.text

      sql._match_str = (confXml \ "neo4j" \ "sql" \ "match").text
      sql._where_str = (confXml \ "neo4j" \ "sql" \ "where").text

      sql._where_exist = !CommonUtil.isNullOrEmpty(sql._where_str)

      var return_metaData = new mutable.LinkedHashMap[String, ArrayBuffer[String]]()
      (confXml \ "neo4j" \ "sql" \ "return") (0).child.foreach(x => {
        if ("field".equals(x.label)) {
          val type_attr: Option[Seq[Node]] = x.attribute("type")
          type_attr match {
            case Some(tp_node) =>
              val field_str: String = x.text
              val splits: Array[String] = field_str.split("\\.")
              if (splits.length != 2) {
                throw new XMLParseException(s"return节点中field节点的${x.text}定义的格式有问题，正确的格式为 ‘别名.字段名’ ")
              }
              val tp: String = tp_node.text
              if ("vertex".equals(tp)) {
                return_metaData.+=(field_str -> null)
              } else if ("edge".equals(tp)) {
                val vertex_name: String = splits(0)
                val field_name = splits(1)
                var fields_array: ArrayBuffer[String] = return_metaData.getOrElse(vertex_name, null)
                if (fields_array == null) {
                  fields_array = new ArrayBuffer[String]()
                  return_metaData.+=(vertex_name -> fields_array)
                }
                fields_array.+=(field_name)
              }
            case None =>
              throw new XMLParseException(s"return节点field节点的${x.text}没有配置type属性，请手动配置")
          }
        }
      })
      sql._return_metaData = return_metaData
      logger.debug(s"returnMetaData:${return_metaData}")

      var return_sql = "RETURN"
      val edge2Sql = new mutable.HashSet[String]()
      return_metaData.foreach {
        case (vertex, fields) =>
          if (fields == null) {
            return_sql = s"${return_sql} ${vertex},"
          } else {
            if (!edge2Sql.contains(vertex)) {
              return_sql = s"${return_sql} ${vertex},"
              edge2Sql.+=(vertex)
            }
          }
      }
      sql._return_str = return_sql.substring(0, return_sql.length - 1)

      if (sql._where_exist) {
        sql._where_conf = (confXml \ "neo4j" \ "sql" \ "where") (0).attributes.asAttrMap
      }

      val where_type: String = sql._where_conf.getOrElse("type", null)

      if ("batch".equals(where_type)) {
        sql._list_conf = (confXml \ "neo4j" \ "sql" \ "list") (0).attributes.asAttrMap
      }

      if ("file".equals(sql._list_conf.getOrElse("type", null))) {
        sql._list_file_reader = new BufferedReader(new InputStreamReader(new FileInputStream(sql._list_conf.getOrElse("path", null))))
      }

      sql._paging_conf = (confXml \ "neo4j" \ "sql" \ "paging") (0).attributes.asAttrMap

    }

    class QuerySQL {
      var _sql_type: String = _

      var _match_str: String = _

      var _where_exist: Boolean = _
      var _where_str: String = _
      var _where_conf: Map[String, String] = _
      var _list_conf: Map[String, String] = _
      var _list_file_reader: BufferedReader = _
      var _list_file_isClose = false

      var _return_str: String = _

      //现阶段不需要使用fields，只是需要根据metaData来解析返回的结果
      //      var _return_fields: mutable.LinkedHashMap[String, String] = _
      var _return_metaData: mutable.LinkedHashMap[String, ArrayBuffer[String]] = _

      var _paging_conf: Map[String, String] = _
    }

  }

}