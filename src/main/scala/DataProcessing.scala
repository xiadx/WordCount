import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.{explode, get_json_object, regexp_replace, split}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.parsing.json.JSON

object DataProcessing {
  val CONTEXT_MAP_PHOTO = Map("travel" -> 1,
                              "guide" -> 3,
                              "qa" -> 2,
                              "weng" -> 0)
  val CONTEXT_MAP = Map("index_note" -> 1,
                        "index_guide" -> 3,
                        "index_question" -> 2,
                        "index_weng_new" -> 0,
                        "index_note_new" -> 1)

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("DataProcessing")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local")
      .appName("DataProcessing")
      .getOrCreate()
//    val spark = SparkSession.builder.config(conf)

//    // 1.
//    val df = spark.read.json("home_article_list_show.json").cache()
//    df.show()

//    // 2.
//    val df = spark.read.json("home_article_list_show.json").cache()
//    df.createOrReplaceTempView("event")
//    val sql =
//      """
//        |select attr from event
//      """.stripMargin
//    val df1 = spark.sql(sql)
//    df1.show()

//    // 3.
//    val df = spark.read.json("home_article_list_show.json").cache()
//    df.createOrReplaceTempView("event")
//    val sql =
//      """
//        |select
//        | attr,
//        | get_json_object(event.attr, '$.item_business_id') as item_business_id from event
//      """.stripMargin
//    val df1 = spark.sql(sql)
//    df1.show()

//    // 4.
//    val df = spark.read.json("home_article_list_show.json").cache()
//    df.createOrReplaceTempView("event")
//    val sql =
//      """
//        |select
//        | dt as imp_dt,
//        | hour as imp_hour,
//        | open_udid imp_open_udid,
//        | attr_abtest_type as imp_attr_abtest_type,
//        | (
//        |   case
//        |   when get_json_object(event.attr, '$.item_business_id') <= 1000 then '<1000'
//        |   else '>1000'
//        |   end
//        | ) as imp_item_business_id_type,
//        | count(1) as pv
//        |from event
//        |group by 1, 2, 3, 4, 5
//      """.stripMargin
//    val df1 = spark.sql(sql)
//    df1.show()

//    // 5.
//    val df = spark.read.json("home_article_list_click.json").cache()
//    df.createOrReplaceTempView("event")
//    val sql =
//      """
//        |select
//        | dt as click_dt,
//        | hour as click_hour,
//        | open_udid as click_open_udid,
//        | attr_abtest_type as click_attr_abtest_type,
//        | (
//        |   case
//        |   when get_json_object(event.attr, '$.item_business_id') <= 1000 then '<1000'
//        |   else '>1000'
//        |   end
//        | ) as click_item_business_id_type,
//        | count(1) as click
//        |from event
//        |group by 1, 2, 3, 4, 5
//      """.stripMargin
//    val df1 = spark.sql(sql)
//    df1.show()

//    // 6.
//    val df1 = spark.read.json("home_article_list_show.json").cache()
//    val df2 = spark.read.json("home_article_list_click.json").cache()
//    df1.createOrReplaceTempView("show")
//    df2.createOrReplaceTempView("click")
//    val sql =
//      """
//        |with show as
//        |(
//        | select
//        |   dt as imp_dt,
//        |   hour as imp_hour,
//        |   open_udid imp_open_udid,
//        |   attr_abtest_type as imp_attr_abtest_type,
//        |   (
//        |     case
//        |     when get_json_object(show.attr, '$.item_business_id') <= 1000 then '<1000'
//        |     else '>1000'
//        |     end
//        |   ) as imp_item_business_id_type,
//        |   count(1) as pv
//        | from show
//        | group by 1, 2, 3, 4, 5
//        |), click as
//        |(
//        | select
//        |   dt as click_dt,
//        |   hour as click_hour,
//        |   open_udid as click_open_udid,
//        |   attr_abtest_type as click_attr_abtest_type,
//        |   (
//        |     case
//        |     when get_json_object(click.attr, '$.item_business_id') <= 1000 then '<1000'
//        |     else '>1000'
//        |     end
//        |   ) as click_item_business_id_type,
//        |   count(1) as click
//        | from click
//        | group by 1, 2, 3, 4, 5
//        |)
//        |select
//        | imp_dt as dt,
//        | imp_hour as hour,
//        | imp_open_udid as open_udid,
//        | imp_attr_abtest_type as abtest_type,
//        | imp_item_business_id_type as item_business_id_type,
//        | pv,
//        | click
//        |from show left join click
//        |on
//        |(
//        | imp_dt = click_dt
//        | and imp_open_udid = click_open_udid
//        | and imp_attr_abtest_type = click_attr_abtest_type
//        | and imp_item_business_id_type = click_item_business_id_type
//        | and imp_hour = click_hour
//        |)
//      """.stripMargin
//    val df3 = spark.sql(sql)
//    df3.show()

//    // 7.
//    val df1 = spark.read.json("home_article_list_show.json").cache()
//    val df2 = spark.read.json("home_article_list_click.json").cache()
//    df1.createOrReplaceTempView("show")
//    df2.createOrReplaceTempView("click")
//    val sql =
//      """
//        |with show as
//        |(
//        | select
//        |   dt as imp_dt,
//        |   hour as imp_hour,
//        |   open_udid imp_open_udid,
//        |   attr_abtest_type as imp_attr_abtest_type,
//        |   (
//        |     case
//        |     when get_json_object(show.attr, '$.item_business_id') <= 1000 then '<1000'
//        |     else '>1000'
//        |     end
//        |   ) as imp_item_business_id_type,
//        |   count(1) as pv
//        | from show
//        | group by 1, 2, 3, 4, 5
//        |), click as
//        |(
//        | select
//        |   dt as click_dt,
//        |   hour as click_hour,
//        |   open_udid as click_open_udid,
//        |   attr_abtest_type as click_attr_abtest_type,
//        |   (
//        |     case
//        |     when get_json_object(click.attr, '$.item_business_id') <= 1000 then '<1000'
//        |     else '>1000'
//        |     end
//        |   ) as click_item_business_id_type,
//        |   count(1) as click
//        | from click
//        | group by 1, 2, 3, 4, 5
//        |)
//        |select
//        | imp_dt as dt,
//        | imp_hour as hour,
//        | imp_open_udid as open_udid,
//        | imp_attr_abtest_type as abtest_type,
//        | imp_item_business_id_type as item_business_id_type,
//        | pv,
//        | click
//        |from show left join click
//        |on
//        |(
//        | imp_dt = click_dt
//        | and imp_open_udid = click_open_udid
//        | and imp_attr_abtest_type = click_attr_abtest_type
//        | and imp_item_business_id_type = click_item_business_id_type
//        | and imp_hour = click_hour
//        |)
//      """.stripMargin
//    val df3 = spark.sql(sql).filter("item_business_id_type = '<1000'").cache()
//    val rankingUserSql =
//      """
//        |select
//        | dt,
//        | open_udid,
//        | 1 as rankingType
//        |from show
//        |group by 1, 2
//      """.stripMargin
//    val df4 = spark.sql(rankingUserSql).cache()
//    import spark.implicits._
//    val ranking = df3.join(df4, Seq("dt", "open_udid"), "left")
//      .filter("rankingType = 1")
//      .groupBy("dt", "abtest_type")
//      .agg("pv" -> "sum", "click" -> "sum")
//      .select($"dt",
//                    $"abtest_type",
//                    $"sum(pv)".as("pv"),
//                    $"sum(click)".as("click"),
//                    ($"sum(click)"/$"sum(pv)").as("ctr"))
//    val noRanking = df3.join(df4, Seq("dt","open_udid"),"left")
//      .filter("rankingType is null")
//      .groupBy("dt","abtest_type")
//      .agg("pv" -> "sum","click" -> "sum")
//      .select($"dt",
//                    $"abtest_type",
//                    $"sum(pv)".as("no_rank_pv") ,
//                    $"sum(click)".as("no_rank_click"),
//                    ($"sum(click)"/$"sum(pv)").as("no_rank_ctr"))
//    ranking.show()
//    noRanking.show()

//    // 8.
//    spark.udf.register("contextType", contextTypeMapping)
//    spark.udf.register("getPhotoId", getPhotoId)
//    spark.udf.register("hasMusic", hasMusic)
//    spark.udf.register("fixMmddMuti", fixMmddMuti)
//    val df1 = spark.read.json("tmp_prefix_recom_item.json").cache()
////    df1.select($"getPhotoId(id)".as("attr_item_business_id"),
////               $"cotextType(type)".as("attr_item_type"),
////               $"ctime",
////               $"mtime",
////               $"hasMusic(music)".as("music"),
////               $"sdate",
////               $"edate",
////               $"fixMmddMuti(mmdd)".as("aMdd")).show() // import spark.implicits._
//    df1.selectExpr("getPhotoId(id) as attr_item_business_id",
//    "contextType(type) as attr_item_type",
//    "ctime",
//    "mtime",
//    "hasMusic(music) as music",
//    "sdate","edate","fixMmddMuti(mmdd) as aMdd").show()

//    // 9.
//    spark.udf.register("contextTypeLog", logContextTypeMapping)
//    val df = spark.read.json("home_article_list_click.json")
//    df.createOrReplaceTempView("home_article_list_click")
//    val homeArticleListClickSql =
//      """
//        |select 1 as action_code, open_udid ,device_type , attr_item_business_id , dt , hour ,contextTypeLog(attr_item_type) as attr_item_type, lat , lng,
//        |   case
//        |       when get_json_object(attr,'$.channel_id') in ('12','13','14','16','18','19','27') then 'indexFlow'
//        |       when get_json_object(attr,'$.channel_id') = 55 then 'onTravel'
//        |       end as  modelType
//        |   from home_article_list_click
//        |where
//        |   dt >= '%s' and dt <= '%s'
//        |   and app_code in ('com.mfw.roadbook','cn.mafengwo.www','cn.mafengwo.www.ipad')
//        |   and open_udid is not null and open_udid != ''
//        |   and attr_item_business_id is not null and attr_item_business_id != ''
//        |   and attr_item_type in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
//      """.stripMargin.format("20181012", "20181014")
//    spark.sql(homeArticleListClickSql).show()

//    // 10.
//    spark.udf.register("contextType", contextTypeMapping)
//    spark.udf.register("getPhotoId", getPhotoId)
//    spark.udf.register("hasMusic", hasMusic)
//    spark.udf.register("fixMmddMuti", fixMmddMuti)
//    val articleProfileDf = spark.read.json("tmp_prefix_recom_item.json").cache()
//    val df1 = articleProfileDf.selectExpr("getPhotoId(id) as attr_item_business_id",
//    "contextType(type) as attr_item_type",
//    "ctime",
//    "mtime",
//    "hasMusic(music) as music",
//    "sdate","edate","fixMmddMuti(mmdd) as aMdd")
//
//    spark.udf.register("contextTypeLog", logContextTypeMapping)
//    val articleClickDf = spark.read.json("home_article_list_click.json")
//    articleClickDf.createOrReplaceTempView("home_article_list_click")
//    val homeArticleListClickSql =
//      """
//        |select 1 as action_code, open_udid ,device_type , attr_item_business_id , dt , hour ,contextTypeLog(attr_item_type) as attr_item_type, lat , lng,
//        |   case
//        |       when get_json_object(attr,'$.channel_id') in ('12','13','14','16','18','19','27') then 'indexFlow'
//        |       when get_json_object(attr,'$.channel_id') = 55 then 'onTravel'
//        |       end as  modelType
//        |   from home_article_list_click
//        |where
//        |   dt >= '%s' and dt <= '%s'
//        |   and app_code in ('com.mfw.roadbook','cn.mafengwo.www','cn.mafengwo.www.ipad')
//        |   and open_udid is not null and open_udid != ''
//        |   and attr_item_business_id is not null and attr_item_business_id != ''
//        |   and attr_item_type in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
//      """.stripMargin.format("20181012", "20181014")
//    val df2 = spark.sql(homeArticleListClickSql)
//
//    df2.join(df1, Seq("attr_item_business_id", "attr_item_type"), "inner").show()

//    // 11.
//    import java.util.Calendar
//    val cal = Calendar.getInstance()
//    val sdf = new java.text.SimpleDateFormat("yyyyMMdd")
//    cal.setTime(sdf.parse("20190210"))
//    cal.add(Calendar.DATE, -30)
//    val startTime_30 = sdf.format(cal.getTime)
//    cal.add(Calendar.DATE, 23)
//    val startTime_7 = sdf.format(cal.getTime)

//    // 12.
//    val df = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df.createOrReplaceTempView("prefix_recom_item")
//    df.select("tags").foreach(line => println(line))

//    val df = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df.createOrReplaceTempView("prefix_recom_item")
//    val sql =
//      """
//        |select get_json_object(tags, "$[0].name") as name from prefix_recom_item
//      """.stripMargin
//    spark.sql(sql).groupBy("name").count().show()

//    // 13.
//    val df = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df.createOrReplaceTempView("prefix_recom_item")
//    spark.sql("desc prefix_recom_item").show()

//    // 14.
//    val df = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df.createOrReplaceTempView("prefix_recom_item")
//    val sql =
//      """
//        |select explode(split(regexp_replace(regexp_replace(tags,'\\[|\\]',''), '\\},', '\\};'),';')) from prefix_recom_item
//      """.stripMargin
//    spark.sql(sql).foreach(line => println(line))

//    // 15.
//    val df1 = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df1.createOrReplaceTempView("prefix_recom_item")
//    val sql1 =
//      """
//        |select explode(split(regexp_replace(regexp_replace(tags,'\\[|\\]',''), '\\},', '\\};'),';')) as tags from prefix_recom_item
//      """.stripMargin
//    val df2 = spark.sql(sql1)
//    df2.createOrReplaceTempView("tmp_prefix_recom_item")
//    val sql2 =
//      """
//        |select get_json_object(tags, "$.type") as type from tmp_prefix_recom_item
//      """.stripMargin
//    spark.sql(sql2).show()

//    // 16.
//    import spark.implicits._
//    val df = Seq(
//      (1, "First Value", java.sql.Date.valueOf("2010-01-01")),
//      (2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
//    ).toDF("int_column", "string_column", "date_column")
//    df.show()

//    // 17.
//    import org.apache.spark.sql.types._
//    val schema = StructType(List(
//      StructField("integer_column", IntegerType, nullable = false),
//      StructField("string_column", StringType, nullable = true),
//      StructField("date_column", DateType, nullable = true)
//    ))
//
//    val rdd = sc.parallelize(Seq(
//      Row(1, "First Value", java.sql.Date.valueOf("2010-01-01")),
//      Row(2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
//    ))
//    val df = spark.createDataFrame(rdd, schema)
//    df.show()

//    // 18.
//    import spark.implicits._
//    case class Person(name: String, age: Int)
//    val people = sc.parallelize(Array("xdx,18", "ybz,20")).map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt)).toDF()
//    people.foreach(line => println(line))

//    // 19.
//    spark.udf.register("contextType", contextTypeMapping)
//    spark.udf.register("getPhotoId", getPhotoId)
//    spark.udf.register("hasMusic", hasMusic)
//    spark.udf.register("fixMmddMuti", fixMmddMuti)
//    val df = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df.createOrReplaceTempView("prefix_recom_item")
//    val sql =
//      """
//        |select
//        | getPhotoId(id) as attr_item_business_id,
//        | contextType(type) as attr_item_type,
//        | ctime,
//        | mtime,
//        | hasMusic(music) as music,
//        | sdate,
//        | edate,
//        | fixMmddMuti(mmdd) as aMdd,
//        | explode(split(regexp_replace(regexp_replace(tags,'\\[|\\]',''), '\\},', '\\};'),';')) as tags
//        |from prefix_recom_item
//      """.stripMargin
//    spark.sql(sql).show()

//    // 19.
//    spark.udf.register("contextType", contextTypeMapping)
//    spark.udf.register("getPhotoId", getPhotoId)
//    spark.udf.register("hasMusic", hasMusic)
//    spark.udf.register("fixMmddMuti", fixMmddMuti)
//    val df = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df.createOrReplaceTempView("prefix_recom_item")
//    val sql =
//      """
//        |select
//        | getPhotoId(id) as attr_item_business_id,
//        | contextType(type) as attr_item_type,
//        | ctime,
//        | mtime,
//        | hasMusic(music) as music,
//        | sdate,
//        | edate,
//        | fixMmddMuti(mmdd) as aMdd,
//        | explode(split(regexp_replace(regexp_replace(tags,'\\[|\\]',''), '\\},', '\\};'),';')) as tags
//        |from prefix_recom_item
//      """.stripMargin
//    val df2 = spark.sql(sql)
//    df2.createOrReplaceTempView("tmp_prefix_recom_item")
//    val sql2 =
//      """
//        |select
//        | attr_item_business_id,
//        | attr_item_type,
//        | get_json_object(tags, "$.type") as tags_type
//        |from tmp_prefix_recom_item
//      """.stripMargin
//    spark.sql(sql2).show()

//    // 20.
//    spark.udf.register("contextType", contextTypeMapping)
//    spark.udf.register("getPhotoId", getPhotoId)
//    spark.udf.register("hasMusic", hasMusic)
//    spark.udf.register("fixMmddMuti", fixMmddMuti)
//    val df = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df.createOrReplaceTempView("prefix_recom_item")
//    val sql =
//      """
//        |select
//        | getPhotoId(id) as attr_item_business_id,
//        | contextType(type) as attr_item_type,
//        | ctime,
//        | mtime,
//        | hasMusic(music) as music,
//        | sdate,
//        | edate,
//        | fixMmddMuti(mmdd) as aMdd,
//        | explode(split(regexp_replace(regexp_replace(tags,'\\[|\\]',''), '\\},', '\\};'),';')) as tags
//        |from prefix_recom_item
//      """.stripMargin
//    val df2 = spark.sql(sql)
//    df2.createOrReplaceTempView("tmp_prefix_recom_item")
//    val sql2 =
//      """
//        |select
//        | attr_item_business_id,
//        | attr_item_type,
//        | get_json_object(tags, "$.type") as tags_type
//        |from tmp_prefix_recom_item
//      """.stripMargin
//    val df3 = spark.sql(sql2)
//
//    spark.udf.register("contextTypeLog", logContextTypeMapping)
//    val articleClickDf = spark.read.json("home_article_list_click.json")
//    articleClickDf.createOrReplaceTempView("home_article_list_click")
//    val homeArticleListClickSql =
//      """
//        |select 1 as action_code, open_udid ,device_type , attr_item_business_id , dt , hour ,contextTypeLog(attr_item_type) as attr_item_type, lat , lng,
//        |   case
//        |       when get_json_object(attr,'$.channel_id') in ('12','13','14','16','18','19','27') then 'indexFlow'
//        |       when get_json_object(attr,'$.channel_id') = 55 then 'onTravel'
//        |       end as  modelType
//        |   from home_article_list_click
//        |where
//        |   dt >= '%s' and dt <= '%s'
//        |   and app_code in ('com.mfw.roadbook','cn.mafengwo.www','cn.mafengwo.www.ipad')
//        |   and open_udid is not null and open_udid != ''
//        |   and attr_item_business_id is not null and attr_item_business_id != ''
//        |   and attr_item_type in ('index_note','index_guide','index_question','index_note_new','index_weng_new')
//      """.stripMargin.format("20181012", "20181014")
//    val df4 = spark.sql(homeArticleListClickSql)
//    val df5 = df3.join(df4, Seq("attr_item_business_id", "attr_item_type"), "inner").cache()
//    df5.show()

//    // 21.
//    import spark.implicits._
//    val df1 = Seq(
//      (1, "First Value", java.sql.Date.valueOf("2010-01-01")),
//      (2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
//    ).toDF("int_column", "string_column", "date_column")
//    val df2 = Seq(
//      (3, "Third Value", java.sql.Date.valueOf("2010-03-01")),
//      (4, "Fourth Value", java.sql.Date.valueOf("2010-04-01"))
//    ).toDF("x", "y", "z")
//    df1.union(df2).show()

//    // 22.
//    import spark.implicits._
//    val df1 = Seq(
//      (1, "First Value", java.sql.Date.valueOf("2010-01-01")),
//      (2, "Second Value", java.sql.Date.valueOf("2010-02-01"))
//    ).toDF("int_column", "string_column", "date_column")
//    val df2 = Seq(
//      (3, "Third Value", java.sql.Date.valueOf("2010-03-01")),
//      (4, "Fourth Value", java.sql.Date.valueOf("2010-04-01"))
//    ).toDF("x", "y", "z")
////    df1.union(df2).rdd.foreach(a => println(a.getClass))
////    df1.union(df2).rdd.foreach(a => println(a.getInt(0) + ":" + a.getString(1) + ":" + a.getDate(2)))
//    df1.union(df2).rdd.map(a => {(a.getInt(0), a.getString(1), a.getDate(2)) -> a}).foreach(a => println(a.getClass))

//    // 23.
//    import spark.implicits._
//    val df1 = Seq(
//      (0, "user_a", "item_1", "2010-01-01"),
//      (0, "user_b", "item_2", "2010-02-01"),
//      (0, "user_c", "item_3", "2010-02-02")
//    ).toDF("action", "user", "item", "date")
//    val df2 = Seq(
//      (1, "user_a", "item_1", "2010-01-01"),
//      (1, "user_a", "item_3", "2010-03-01"),
//      (1, "user_b", "item_1", "2010-04-01")
//    ).toDF("f", "x", "y", "z")
////    df1.union(df2).rdd.map(a => {(a.getString(1), a.getString(2)) -> a}).foreach(a => println(a))
////    df1.union(df2).rdd.map(a => {(a.getString(1), a.getString(2)) -> a})
////      .reduceByKey((a, b) => {
////        if (a.getInt(0) > 0) {
////          a
////        } else {
////          b
////        }
////      })
////      .foreach(a => println(a))
//    df1.union(df2).rdd.map(a => {(a.getString(1), a.getString(2)) -> a})
//      .reduceByKey((a, b) => {
//        if (a.getInt(0) > 0) {
//          a
//        } else {
//          b
//        }
//      })
//      .map(a => a._2)
//      .foreach(a => println(a))

//    // 24.
//    spark.udf.register("contextType", contextTypeMapping)
//    spark.udf.register("getPhotoId", getPhotoId)
//    spark.udf.register("hasMusic", hasMusic)
//    spark.udf.register("fixMmddMuti", fixMmddMuti)
//    spark.udf.register("getTagsType", getTagsType)
//    val df = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df.createOrReplaceTempView("prefix_recom_item")
//    val sql =
//      """
//        |select
//        | getPhotoId(id) as attr_item_business_id,
//        | contextType(type) as attr_item_type,
//        | ctime,
//        | mtime,
//        | hasMusic(music) as music,
//        | sdate,
//        | edate,
//        | fixMmddMuti(mmdd) as aMdd,
//        | getTagsType(tags) as tags_type
//        |from prefix_recom_item
//      """.stripMargin
//    spark.sql(sql).show()

//    // 25.
//    import spark.implicits._
//    val df = sc.parallelize(Seq(
//      (0, "First Value", "20180101"),
//      (1, "Second Value", "20180101")
//    )).toDF("int_column", "str_column", "date_column")
////    df.filter("str_column = 'First Value'").show()
//    df.filter("str_column in ('First Value', 'Second Value')").show()

//    // 26.
//    import spark.implicits._
//    import org.apache.spark.sql.functions.col
//    val df = sc.parallelize(Seq(
//      (0, "First Value", "20180101"),
//      (1, "Second Value", null)
//    )).toDF("int_column", "str_column", "date_column")
////    df.filter("date_column is null").show()
////    df.filter("date_column is not null").show()
//    df.filter(col("date_column").isNotNull).show()

//    // 27.
//    import spark.implicits._
//    val df = Seq(
//      (0, "First", "20190220"),
//      (1, "Second", "20190221")
//    ).toDF("a", "b", "c")
//    df.as("df1").crossJoin(df.as("df2")).show()

//    // 28.
//    import spark.implicits._
//    spark.udf.register("contextType", contextTypeMapping)
//    spark.udf.register("getPhotoId", getPhotoId)
//    spark.udf.register("hasMusic", hasMusic)
//    spark.udf.register("fixMmddMuti", fixMmddMuti)
//    spark.udf.register("getTagsType", getTagsType)
//    val df = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df.selectExpr("getPhotoId(id) as attr_item_business_id",
//      "contextType(type) as attr_item_type",
//      "ctime",
//      "mtime",
//      "hasMusic(music) as music",
//      "sdate",
//      "edate",
//      "fixMmddMuti(mmdd) as aMdd",
//      "tags")
//    .withColumn("simple_tags", explode(split(regexp_replace(regexp_replace($"tags", "\\[|\\]",""), "\\},", "\\};"),";")))
//    .select($"attr_item_business_id",
//      $"attr_item_type",
//      get_json_object($"simple_tags", "$.name").as("tags_name"),
//      get_json_object($"simple_tags", "$.type").as("tags_type"),
//      get_json_object($"simple_tags", "$.weight").as("tags_weight")
//    ).show()

//    // 29.
//    import spark.implicits._
//    import org.apache.spark.sql.functions.lit
//    val df = Seq(
//      (1, 2),
//      (3, 4)
//    ).toDF("a", "b")
//    df.withColumn("c", lit(6)).show()

//    // 30.
//    import spark.implicits._
//    spark.udf.register("contextType", contextTypeMapping)
//    spark.udf.register("getPhotoId", getPhotoId)
//    spark.udf.register("hasMusic", hasMusic)
//    spark.udf.register("fixMmddMuti", fixMmddMuti)
//    spark.udf.register("getTagsType", getTagsType)
//    val df = spark.read.json("tmp_prefix_recom_item.json").cache()
//    df.selectExpr("getPhotoId(id) as attr_item_business_id",
//      "contextType(type) as attr_item_type",
//      "tags")
//    .withColumn("simple_tags", explode(split(regexp_replace(regexp_replace($"tags", "\\[|\\]",""), "\\},", "\\};"),";")))
//    .select($"attr_item_business_id",
//      $"attr_item_type",
//      get_json_object($"simple_tags", "$.name").as("tags_name"),
//      get_json_object($"simple_tags", "$.type").as("tags_type"),
//      get_json_object($"simple_tags", "$.weight").as("tags_weight")
//    ).filter("tags_weight >= 0.9").show()

//    // 31.
//    import spark.implicits._
//    import org.apache.spark.sql.functions.max
//    val df = Seq(
//      ("a", "20190202", 0.8),
//      ("a", "20190203", 0.6),
//      ("c", "20190204", 0.9),
//      ("c", "20190205",0.1)
//    ).toDF("id", "date", "rate")
//    df.drop("date").groupBy("id").agg(max($"rate").as("rate")).
//      join(df, Seq("id", "rate"), "left").show()

//    // 32.
//    val m = Constant.MOBILE_EVENT_TO_HOME_SHOW_MAPPING
//    val df = spark.read.json("mobile_event_parquet.json").cache()
//    df.createOrReplaceTempView("mobile_event_parquet")
//    val tableName = "mobile_event_parquet"
//    val filterCondition = "event_code = 'show_index'"
//    val map = Constant.MOBILE_EVENT_TO_HOME_SHOW_MAPPING
//    var sql = "select "
//    for ((k, v) <- m) {
//      sql = sql + k + " as " + v + ", "
//    }
//    sql = sql.dropRight(2) + " from " + tableName + " where " + filterCondition
//    spark.sql(sql).show()

//    // 33.
//    val m = Constant.MOBILE_EVENT_TO_HOME_SHOW_MAPPING
//    val df = spark.read.json("mobile_event_parquet.json").cache()
//    df.createOrReplaceTempView("mobile_event_parquet")
//    val tableName = "mobile_event_parquet"
//    val filterCondition = "event_code = 'show_index'"
//    val map = Constant.MOBILE_EVENT_TO_HOME_SHOW_MAPPING
//    var sql = "select "
//    for ((k, v) <- m) {
//      sql = sql + k + " as " + v + ", "
//    }
//    sql = sql.dropRight(2) + " from " + tableName + " where " + filterCondition
//    spark.sql(sql).select("attr_item_type").show(500,false)

//    // 34.
//    import spark.implicits._
//    import org.apache.spark.ml.feature.Word2Vec
//    import org.apache.spark.sql.functions.col
//    val df = Seq(
//      (0, "Hi"),
//      (0, "I"),
//      (0, "heard"),
//      (0, "about"),
//      (0, "Spark"),
//      (1, "I"),
//      (1, "wish"),
//      (1, "Java"),
//      (1, "could"),
//      (1, "use"),
//      (1, "case"),
//      (1, "classes"),
//      (2, "Logistic"),
//      (2, "regression"),
//      (3, "models"),
//      (3, "are"),
//      (3, "neat")
//    ).toDF("id", "feature").filter(!col("feature").isin(Seq("I", "could"): _*))
//    val word2VecDf = df.groupBy("id")
//      .agg("feature" -> "collect_list")
//      .select($"collect_list(feature)".as("text"))
//    val word2Vec = new Word2Vec()
//      .setInputCol("text")
//      .setMaxIter(30)
//      .setOutputCol("result")
//      .setMinCount(0)
//    val model = word2Vec.fit(word2VecDf)
//    val result = model.transform(word2VecDf)
//    result.show()

//    // 35.
//    import spark.implicits._
//    import org.apache.spark.ml.feature.Word2Vec
//    import org.apache.spark.sql.functions.col
//    val df = Seq(
//      (0, "Hi"),
//      (0, "I"),
//      (0, "heard"),
//      (0, "about"),
//      (0, "Spark"),
//      (1, "I"),
//      (1, "wish"),
//      (1, "Java"),
//      (1, "could"),
//      (1, "use"),
//      (1, "case"),
//      (1, "classes"),
//      (2, "Logistic"),
//      (2, "regression"),
//      (3, "models"),
//      (3, "are"),
//      (3, "neat")
//    ).toDF("id", "feature")
//    import scala.collection.mutable.ArrayBuffer
//    var ids = ArrayBuffer[Int]()
//    df.take(2).foreach { case Row(id: Int, feature: String) => ids += id }
//    println(ids)

//    // 36.
//    import spark.implicits._
//    val df1 = Seq(
//      (0, "Hi"),
//      (0, "I"),
//      (0, "heard"),
//      (0, "about"),
//      (0, "Spark")
//    ).toDF("id", "text")
//    val df2 = Seq(
//      (1, "I"),
//      (1, "wish"),
//      (1, "Java"),
//      (1, "could"),
//      (1, "use"),
//      (1, "case"),
//      (1, "classes")
//    ).toDF("id", "text")
//    df1.union(df2).show()

//    // 37.
//    import spark.implicits._
//    val df1 = Seq(
//      (0, "Hi"),
//      (0, "I"),
//      (0, "heard"),
//      (0, "about"),
//      (0, "Spark")
//    ).toDF("id", "text")
//    df1.sample(0.6).show()

//    // 38.
//    import spark.implicits._
//    val df1 = Seq(
//      (0, "Hi"),
//      (0, "I"),
//      (0, "heard"),
//      (0, "about"),
//      (0, "Spark")
//    ).toDF("id", "text")
//    val df2 = Seq(
//      (1, "I"),
//      (1, "wish"),
//      (1, "Java"),
//      (1, "could"),
//      (1, "use"),
//      (1, "case"),
//      (1, "classes")
//    ).toDF("id", "text")
//    df1.union(df2).sample(0.8).show()

//    // 39.
//    import spark.implicits._
//    val df1 = Seq(
//      (0, "Hi"),
//      (0, "I"),
//      (0, "heard"),
//      (0, "about"),
//      (0, "Spark")
//    ).toDF("id", "text")
//    spark.createDataFrame(sc.parallelize(df1.take(2)), schema = df1.schema).show()

//    // 40.
//    import spark.implicits._
//    val df1 = Seq(
//      (0, "Hi"),
//      (0, "I"),
//      (0, "heard"),
//      (0, "about"),
//      (0, "Spark")
//    ).toDF("id", "text")
//    val df2 = spark.createDataFrame(sc.parallelize(df1.take(2)), schema = df1.schema)
//    val df3 = spark.createDataFrame(df1.rdd.subtract(df2.rdd), schema = df1.schema)
//    df3.show()

//      // 41.
//      import spark.implicits._
//      val df1 = Seq(
//        (0, "Hi"),
//        (1, "I")
//      ).toDF("id", "text")
//      val df2 = Seq(
//        (0, "Hi"),
//        (1, "I"),
//        (2, "Java")
//      ).toDF("id", "text")
//      df1.join(df2, Seq("id"), "left").show()

//    // 42.
//    import spark.implicits._
//    import org.apache.spark.sql.expressions.Window
//    import org.apache.spark.sql.functions.{dense_rank, sum}
//    val orders = Seq(
//      ("o1", "s1", "2017-05-01", 100),
//      ("o2", "s1", "2017-05-02", 200),
//      ("o3", "s2", "2017-05-01", 300)
//    ).toDF("order_id", "seller_id", "pay_time", "price")
//    val rankSpec = Window.partitionBy("seller_id").orderBy("pay_time")
//    val shopOrderRank = orders.withColumn("rank", dense_rank.over(rankSpec))
//    shopOrderRank.show()

//    // 43.
//    import spark.implicits._
//    import org.apache.spark.sql.expressions.Window
//    import org.apache.spark.sql.functions.{dense_rank, sum}
//    val orders = Seq(
//      ("o1", "s1", "2017-05-01", 100),
//      ("o2", "s1", "2017-05-02", 200),
//      ("o3", "s2", "2017-05-01", 300)
//    ).toDF("order_id", "seller_id", "pay_time", "price")
//    val sumSpec = Window.partitionBy("seller_id").orderBy("pay_time")
//      .rowsBetween(-1, 0)
//    orders.withColumn("cumulative_sum", sum("price").over(sumSpec)).show()

//    // 44.
//    import spark.implicits._
//    import org.apache.spark.sql.expressions.Window
//    import org.apache.spark.sql.functions.row_number
//    val orders = Seq(
//      ("o1", "s1", "2017-05-01", 100),
//      ("o2", "s1", "2017-05-02", 200),
//      ("o3", "s2", "2017-05-01", 300)
//    ).toDF("order_id", "seller_id", "pay_time", "price")
//    val window = Window.partitionBy("seller_id").orderBy("pay_time")
//    orders.withColumn("num", row_number().over(window)).show()

//    // 45.
//    import spark.implicits._
//    import org.apache.spark.sql.expressions.Window
//    import org.apache.spark.sql.functions.{row_number, dense_rank, rank}
//    val orders = Seq(
//      ("o1", "s1", "2017-05-01", 100),
//      ("o2", "s1", "2017-05-02", 100),
//      ("o3", "s1", "2017-05-03", 200),
//      ("o3", "s2", "2017-05-01", 300)
//    ).toDF("order_id", "seller_id", "pay_time", "price")
//    val window = Window.partitionBy("seller_id").orderBy("price")
//    orders.withColumn("row_number", row_number().over(window)).show()
//    orders.withColumn("dense_rank", dense_rank().over(window)).show()
//    orders.withColumn("rank", rank().over(window)).show()

//    // 46.
//    import spark.implicits._
//    val df = Seq(
//      ("user1", "item1", 1),
//      ("user1", "item1", 2),
//      ("user2", "item2", 3)
//    ).toDF("userid", "itemid", "time")
//    df.rdd.map(r => {
//      (r.getAs[String]("userid"), r.getAs[String]("itemid")) -> r
//    }).reduceByKey((a, b) => {
//      if (a.getAs[Int]("time") > b.getAs[Int]("time")) {
//        a
//      } else {
//        b
//      }
//    }).map(a => {
//      (a._2.getAs[String]("userid"), a._2.getAs[String]("itemid"), a._2.getAs[Int]("time"))
//    }).toDF("userid", "itemid", "time").show()

//    // 47.
//    import spark.implicits._
//    val user_similarity = Seq(
//      ("user1", "user2", 0.8),
//      ("user1", "user3", 0.7),
//      ("user2", "user1", 0.8)
//    ).toDF("auid", "buid", "similarity")
//    val user_action = Seq(
//      ("user2", "item1"),
//      ("user2", "item2"),
//      ("user3", "item2"),
//      ("user3", "item3")
//    ).toDF("uid", "iid")
//    val online_item = Seq(
//      ("item1"),
//      ("item2")
//    ).toDF("oiid")
//    user_action.join(online_item, $"iid" === $"oiid", "inner")
//      .groupBy("uid")
//      .agg("iid" -> "collect_list")
//      .select($"uid", $"collect_list(iid)".as("items")).printSchema()

//    // 48.
//    import spark.implicits._
//    val df = Seq(
//      ("o1", "s1"),
//      ("o1", "s2"),
//      ("o3", "s3")
//    ).toDF("o", "s")
//    df.rdd.map(r => {
//      r.getAs[String]("o") -> r
//    }).groupByKey().foreach(r => println(r))

//    // 49.
//    import spark.implicits._
//    import scala.collection.mutable.HashSet
//    val user_similarity = Seq(
//      ("user1", "user2", 0.8),
//      ("user1", "user3", 0.7),
//      ("user2", "user1", 0.8)
//    ).toDF("auid", "buid", "similarity")
//    val user_action = Seq(
//      ("user2", "item1"),
//      ("user2", "item2"),
//      ("user3", "item2"),
//      ("user3", "item3")
//    ).toDF("uid", "iid")
//    val online_item = Seq(
//      ("item1"),
//      ("item2")
//    ).toDF("oiid")
//    val online_user_action = user_action.join(online_item, $"iid" === $"oiid", "inner")
//      .groupBy("uid")
//      .agg("iid" -> "collect_list")
//      .select($"uid", $"collect_list(iid)".as("items"))
//    user_similarity.join(online_user_action, $"buid" === $"uid", "left")
//      .rdd.map(r => {
//        r.getAs[String]("auid") -> r
//      }).groupByKey().map(a => {
//        val user_sorted = a._2.toArray.sortBy(k => k.getAs[Double]("similarity"))
//        val all_items = new HashSet[String]()
//        user_sorted.foreach(i => {
//          val items = i.getAs[Seq[String]]("items")
//          if (null != items && !items.isEmpty) {
//            all_items ++= items
//          }
//        })
//        val item_array = all_items.toArray
//        var index = item_array.length
//        a._1 -> item_array.map(i => {
//          index = index - 1
//          i + "_" + index
//        })
//     }).foreach {
//      case Tuple2(userid: String, items: Array[String]) => println(userid + "," + items.mkString(";"))
//    }

//    // 50.
//    import com.fasterxml.jackson.databind.ObjectMapper
//    import com.fasterxml.jackson.module.scala.DefaultScalaModule
//    import spark.implicits._
//    val df = Seq(
//      ("o1", "s1"),
//      ("o1", "s2"),
//      ("o3", "s3")
//    ).toDF("o", "s")
//    val cols = df.columns
//    df.foreach(i => {
//      val mapper = new ObjectMapper()
//      mapper.registerModule(DefaultScalaModule)
//      println(mapper.writeValueAsString(i.getValuesMap(cols)))
//    })

//    // 51.
//    import spark.implicits._
//    val df = Seq(
//      ("o1", "s1", "t1"),
//      ("o2", "s2", "t2"),
//      ("o3", "s3", "t3")
//    ).toDF("o", "s", "t")
//    println(df.rdd.map(i => (i.get(0), (i.get(1), i.get(2)))).collect().toMap)

    // 52.






  }

  def getTagsType = (str: String) => {
    try {
      val li = JSON.parseFull(str).get.asInstanceOf[List[Map[String, String]]]
      for (l <- li) {
        println(l("name"))
        println(l("type"))
        println(l("weight"))
        println("==============")
      }
//      println(str)
//      val tagsWeightMap = scala.collection.mutable.Map[String, Double]
//      for (map <- JSON.parseFull(str).get.asInstanceOf[List[Map[String, String]]]) {
//        println(map)
//        val t = map("type")
//        val weight = map("weight")
//        println(t)
//        println(t.getClass)
//        println(weight)
//        print(weight.getClass)
//
//        if (tagsWeightMap.contains(t)) {
//          tagsWeightMap(t) = (tagsWeightMap(t) + weight.toDouble)
//        } else {
//          tagsWeightMap(t) = weight.toDouble
//        }
//      }
      val pattern = "\"type\":\"\\w*\"".r
      pattern.findFirstIn(str).get.replace("\"", "").split(":")(1)
    } catch {
      case _: Throwable => "unknown"
    }
  }

  def contextTypeMapping = (context_type: String) => {
    CONTEXT_MAP_PHOTO.getOrElse(context_type, 0)
  }

  def getPhotoId = (str: String) => {
    try {
      str.split("_")(1).toLong
    } catch {
      case _: Throwable => { 0 }
    }
  }

  def hasMusic = (str: String) => {
    if (!StringUtils.isEmpty(str)) {
      0
    } else {
      1
    }
  }

  def fixMmddMuti = (mmid : String) => {
    var rst = mmid
    if(StringUtils.isNotEmpty(mmid) && mmid.contains(",")){
      try {
        rst = mmid.split(",")(0)
      }catch {
        case _: Throwable => {rst = "error" + mmid}
      }
    }
    rst
  }

  def logContextTypeMapping= (context_type: String) => {
    CONTEXT_MAP.getOrElse(context_type, 0)
  }
}
























