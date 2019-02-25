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

    // 5.
    println("Hello World".split(" ").getClass)

  }
}
