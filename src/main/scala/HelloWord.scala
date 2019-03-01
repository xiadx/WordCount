import Constant.MOBILE_EVENT_TO_HOME_SHOW_MAPPING

import scala.collection.mutable.ArrayBuffer

object HelloWord {
  def main(args: Array[String]): Unit = {
//    // 1.
//    import java.util.Calendar
//    val cal = Calendar.getInstance()
//    val sdf = new java.text.SimpleDateFormat("yyyyMMdd")
//    cal.setTime(sdf.parse("20190210"))
//    cal.add(Calendar.DATE, -30)
//    val startTime_30 = sdf.format(cal.getTime)
//    println(startTime_30)

//    // 2.
//    println(Array("a", "a", "b", "c", "d", "d").groupBy(a => a)
//        .map(a => a._1 + "=" + a._2.length).mkString(";"))

//    // 3.
//    import scala.util.matching.Regex
//    val pattern = "\"name\":\"\\w*\"|[\\u4e00-\\u9fa5]\"".r
//    val str = """[{"name":"xdx"}]""""
//    val name = pattern.findFirstIn(str).get.replace("\"", "").split(":")(1)
//    println(name)

//    // 4.
//    val t = Tuple1(1, 2, 3)
//    println(t)

//    // 5.
//    println("Hello World".split(" ").getClass)

//    // 6.
//    for ((k, v) <- Constant.MOBILE_EVENT_TO_HOME_SHOW_MAPPING) {
//      println(k + ":" + v)
//    }

//    // 7.
//    val tableName = "mobile_event_parquet"
//    val map = Constant.MOBILE_EVENT_TO_HOME_SHOW_MAPPING
//    var sql = "select "
//    for ((k, v) <- map) {
//      sql = sql + k + " as " + v + ", "
//    }
//    sql = sql.dropRight(2) + " from " + tableName
//    println(sql)

//    // 8.
//    val mapObj = Map("username" -> "xdx", "age" -> 18)
//    println(mapObj.getClass)
//    println(mapObj.get("username").getClass)
//    println(mapObj.get("username"))
//    println(mapObj.getOrElse("username", "default"))
//    for ((k, v) <- mapObj) {
//      println(k.getClass)
//      println(v.getClass)
//      println(k + ":" + v)
//    }

//    // 9.
//    import scala.util.parsing.json.JSON
//    import scala.util.parsing.json.JSONObject
//    val colors: Map[String, Object] = Map("red" -> "123456", "azure" -> "789789")
//    val json = JSONObject(colors)
//    println(json.getClass)
//    println(json)
//    println(json.toString())
//    val jsonMap = JSON.parseFull(json.toString())
//    println(jsonMap.getClass)
//    println(jsonMap)
//    println(jsonMap.get.getClass)
//    println(jsonMap.get)
//    println(jsonMap.get.asInstanceOf[Map[String, Object]].getClass)
//    val jsonStr = """{"username":"Ricky", "age":"21"}"""
//    val jsonValue = JSON.parseFull(jsonStr)
//    val jsonObj = jsonValue match {
//      case Some(map:Map[String, Any]) => map.asInstanceOf[Map[String, String]]
//      case _ => Map("username" -> "default", "age" -> "default")
//    }
//    println(jsonObj.get("username"))
//    println(jsonObj.getOrElse("age", 0))

//    // 10.
//    import scala.util.parsing.json.JSON
//    val jsonStr = """{"username":"Ricky", "attribute":{"age":21, "weight": 60}}"""
//    val jsonValue = JSON.parseFull(jsonStr)
//    println(jsonValue)
//    val jsonObj = jsonValue match {
//      case Some(map:Map[String, Any]) => map
//      case other => Map[String, Any]()
//    }
//    println(jsonObj.get("attribute").get.asInstanceOf[Map[String, String]].get("age"))

    // 11.


  }


























}
