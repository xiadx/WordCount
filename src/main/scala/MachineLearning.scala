import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MachineLearning {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("MachineLearning")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

//    // 1.
//    import spark.implicits._
//    val data = Seq(
//      (1, 2, 3),
//      (4, 5, 6)
//    )
//    val df =  data.toDF("a", "b", "c")
//    df.show()

//    // 2.
//    import spark.implicits._
//    val data = Seq(
//      Tuple1(1, 2, 3),
//      Tuple1(4, 5, 6)
//    )
//    val df = data.toDF("feature")
//    df.show()

//    // 3.
//    import spark.implicits._
//    val data = Seq(
//      Tuple1(1, 2, 3),
//      Tuple1(4, 5, 6)
//    )
//    val df = data.toDF("feature")
//    df.foreach(row => println(row))
//    df.foreach(row => println(row.getClass))
//    df.foreach(row => println(row.getAs(0)))
//    df.foreach(row => println(row.getAs(0).getClass))

//    // 4.
//    import org.apache.spark.ml.linalg.Vectors
//    import spark.implicits._
//    val data = Seq(
//      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
//      Vectors.dense(4.0, 5.0, 0.0, 3.0),
//      Vectors.dense(6.0, 7.0, 0.0, 8.0),
//      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
//    )
//    val df = data.map(Tuple1.apply).toDF("feature")
//    df.show()

//    // 5.
//    import org.apache.spark.ml.linalg.{Matrix, Vectors}
//    import org.apache.spark.ml.stat.Correlation
//    import org.apache.spark.sql.Row
//    import spark.implicits._
//    val data = Seq(
//      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
//      Vectors.dense(4.0, 5.0, 0.0, 3.0),
//      Vectors.dense(6.0, 7.0, 0.0, 8.0),
//      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
//    )
//    val df = data.map(Tuple1.apply).toDF("features")
//    val Row(coeff1: Matrix) = Correlation.corr(df, "features").head
//    println(s"Pearson correlation matrix:\n $coeff1")
//    val Row(coeff2: Matrix) = Correlation.corr(df, "features", "spearman").head
//    println(s"Spearman correlation matrix:\n $coeff2")

//    // 6.
//    import org.apache.spark.ml.linalg.{Vector, Vectors}
//    import org.apache.spark.ml.stat.ChiSquareTest
//    import spark.implicits._
//    val data = Seq(
//      (0.0, Vectors.dense(0.5, 10.0)),
//      (0.0, Vectors.dense(1.5, 20.0)),
//      (1.0, Vectors.dense(1.5, 30.0)),
//      (0.0, Vectors.dense(3.5, 30.0)),
//      (0.0, Vectors.dense(3.5, 40.0)),
//      (1.0, Vectors.dense(3.5, 40.0))
//    )
//    val df = data.toDF("label", "features")
//    val chi = ChiSquareTest.test(df, "features", "label").head
//    println(s"pValues = ${chi.getAs[Vector](0)}")
//    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
//    println(s"statistics ${chi.getAs[Vector](2)}")

//    // 7.
//    import org.apache.spark.ml.linalg.{Vector, Vectors}
//    import org.apache.spark.ml.stat.Summarizer.metrics
//    import spark.implicits._
//    val data = Seq(
//      (Vectors.dense(2.0, 3.0, 5.0), 1.0),
//      (Vectors.dense(4.0, 6.0, 7.0), 2.0)
//    )
//    val df = data.toDF("features", "weight")
//    df.select(metrics("mean", "variance")
//                .summary($"features", $"weight")
//                .as("summary")).foreach(row => println(row))

//    // 8.
//    import org.apache.spark.ml.linalg.{Vector, Vectors}
//    import org.apache.spark.ml.stat.Summarizer.metrics
//    import spark.implicits._
//    val data = Seq(
//      (Vectors.dense(2.0, 3.0, 5.0), 1.0),
//      (Vectors.dense(4.0, 6.0, 7.0), 2.0)
//    )
//    val df = data.toDF("features", "weight")
//    df.select(metrics("mean", "variance")
//                .summary($"features", $"weight")
//                .as("summary"))
//      .select("summary.mean", "summary.variance")
//      .show()

//    // 9.
//    import org.apache.spark.ml.linalg.{Vector, Vectors}
//    import org.apache.spark.ml.stat.Summarizer.metrics
//    import org.apache.spark.ml.stat.Summarizer.mean
//    import org.apache.spark.ml.stat.Summarizer.variance
//    import spark.implicits._
//    val data = Seq(
//      (Vectors.dense(2.0, 3.0, 5.0), 1.0),
//      (Vectors.dense(4.0, 6.0, 7.0), 2.0)
//    )
//    val df = data.toDF("features", "weight")
//    val (meanVal, varianceVal) = df.select(metrics("mean", "variance")
//                .summary($"features", $"weight")
//                .as("summary"))
//      .select("summary.mean", "summary.variance")
//      .as[(Vector, Vector)]
//      .first()
//    println(s"with weight: mean = ${meanVal}, variance = ${varianceVal}")
//    val (meanVal2, varianceVal2) = df.select(mean($"features"), variance($"features"))
//      .as[(Vector, Vector)].first()
//    println(s"without weight: mean = ${meanVal2}, sum = ${varianceVal2}")

//    // 10.
//    val df = spark.read.format("image").option("dropInvalid", true).load("data/mllib/images/origin/kittens")
//    df.select("image.origin", "image.width", "image.height").show(truncate=false)

    // 11.
    import org.apache.spark.ml.feature.Word2Vec
    import org.apache.spark.ml.linalg.Vector
    import org.apache.spark.sql.Row
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" ")
    ).map(Tuple1.apply)).toDF("text")
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)
    model.getVectors.show()


  }

}
