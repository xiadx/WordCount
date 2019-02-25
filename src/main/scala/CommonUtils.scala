import java.nio.file.{Paths, Files}
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import java.io.FileWriter
import scala.util.parsing.json.JSON
import scala.util.parsing.json.JSONObject

object CommonUtils {
  def main(args: Array[String]): Unit = {
//    val contentPath = "home_article_list_click.txt"
//    val descPath = "desc_home_article_list_click.txt"
//    val savePath = "home_article_list_click.json"
//    val contentPath = "home_article_list_show.txt"
//    val descPath = "desc_home_article_list_show.txt"
//    val savePath = "home_article_list_show.json"
    val contentPath = "tmp_prefix_recom_item.txt"
    val descPath = "desc_tmp_prefix_recom_item.txt"
    val savePath = "tmp_prefix_recom_item.json"
    generateJsonData(contentPath, descPath, savePath)
//    scalaJsonTest()
  }

  def generateJsonData(contentPath: String, descPath: String, saveFile: String): Unit = {
    if (!Files.exists(Paths.get(contentPath)) || !Files.exists(Paths.get(descPath))) {
      return
    }
    val descLines = Source.fromFile(descPath, "UTF-8").getLines()
    val keys: ArrayBuffer[String] = ArrayBuffer()
    for (line <- descLines) {
      val values = line.split("\t")
      keys += values(0)
    }
    val outFile = new FileWriter(saveFile, true)
    val contentLines = Source.fromFile(contentPath, "UTF-8").getLines()
    for (line <- contentLines) {
      var records = Map[String, String]()
      val values = line.split("\t")
      for (i <- 0 until values.length) {
        records += (keys(i).trim() -> values(i).trim())
      }
      outFile.write(JSONObject(records).toString() + "\n")
    }
    outFile.close()
  }

  def scalaJsonTest(): Unit = {
    val m = Map("A" -> 1, "B" -> 2)
    println(m)
    val json = JSONObject(m)
    println(json)
    println(json.getClass)
    val jsonStr = json.toString()
    println(jsonStr)
    val jsonValue = JSON.parseFull(jsonStr)
    val jsonObj = jsonValue match {
      case Some(map: Map[String, Any]) => map.asInstanceOf[Map[String, Int]]
      case _ => println("ERROR jsonStr")
    }
    println(jsonObj)
    println(jsonObj.getClass)
  }
}
