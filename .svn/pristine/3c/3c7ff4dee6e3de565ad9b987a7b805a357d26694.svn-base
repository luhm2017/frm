package com.lakala.finance.common

import java.io.{Closeable, PrintWriter, StringWriter}
import javax.sql.DataSource

/**
  * Created by longxiaolei on 2017/6/15.
  */
object CommonUtil {
  def isNullOrEmpty(str: String): Boolean = {
    return null == str || "".equals(str.trim)
  }

  //关闭各种IO流，避免写if判空的逻辑
  def closeIO(streams: AutoCloseable*) = {
    streams.foreach(stream => {
      if (stream != null) {
        stream.close()
      }
    })
  }

  def getExceptionInfo(exception: Exception): String = {
    var out: StringWriter = null
    var pw: PrintWriter = null
    try {
      out = new StringWriter()
      pw = new PrintWriter(out)
      exception.printStackTrace(pw)
      out.toString
    } finally {
      closeIO(out)
      closeIO(pw)
    }
  }

}
