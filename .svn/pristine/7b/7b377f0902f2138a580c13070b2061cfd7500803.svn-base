import java.util.concurrent.atomic.AtomicInteger
import java.util.regex.{Matcher, Pattern}

import com.alibaba.fastjson.JSON

import scala.collection.mutable
import scala.util.matching.Regex

/**
  * Created by longxiaolei on 2017/6/16.
  */
object TestFasterJson {
  val Vertex_partten = new Regex("^v\\d*$")

  val vertex_match: Pattern = Pattern.compile("^v\\d*$")

  def main(args: Array[String]) {
    //    val json = "{\"id\":2311873, \"labels\":[\"ApplyInfo\"], \"contract_no\":\"XNA201608177742587\", \"cert_no\":\"520102197204283412\", \"orderno\":\"XNA20160817110844748684\", \"device_id\":\"867303021911409\", \"comp_addr\":\"贵阳市\\南明区瑞金南路中创联合大厦18楼\", \"modelname\":\"ApplyInfo\", \"mobile\":\"13885044977\", \"insert_time\":\"2016-08-17 11:36:27.215\", \"term_id\":\"CBC3A11005633655\", \"ipv4\":\"1.204.29.219\", \"user_id\":\"7742587\", \"return_pan\":\"6212262402013084583\", \"contact_mobile\":\"13511941263\", \"comp_phone\":\"0851-85667841\", \"company\":\"贵阳市金佰佳装装饰有限公司\", \"tag\":\"black\", \"loan_pan\":\"6212262402013084583\", \"email\":\"13885044977@163.com\"}"
    //    val json1 = JSON.parse(json)
    //    println(json1)
    val temp: mutable.HashMap[String, Array[String]] = new mutable.HashMap[String, Array[String]]()

//    val result: Boolean = vertex_match.matcher("v22").matches()
    //    println("vV0111".matches("^[v|V][0-9]*"))
//    println(temp)


    val point: AtomicInteger = new AtomicInteger(0)

    val get: Int = point.addAndGet(100)
    println(get)

  }
}
