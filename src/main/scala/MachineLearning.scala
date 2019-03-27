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

//    // 11.
//    import org.apache.spark.ml.feature.Word2Vec
//    import org.apache.spark.ml.linalg.Vector
//    import org.apache.spark.sql.Row
//    val documentDF = spark.createDataFrame(Seq(
//      "Hi I heard about Spark".split(" "),
//      "I wish Java could use case classes".split(" "),
//      "Logistic regression models are neat".split(" ")
//    ).map(Tuple1.apply)).toDF("text")
//    val word2Vec = new Word2Vec()
//      .setInputCol("text")
//      .setOutputCol("result")
//      .setVectorSize(3)
//      .setMinCount(0)
//    val model = word2Vec.fit(documentDF)
//    model.getVectors.show()

//    // 12.
//    import org.apache.spark.ml.classification.LogisticRegression
//    import org.apache.spark.ml.linalg.{Vector, Vectors}
//    import org.apache.spark.ml.param.ParamMap
//    import org.apache.spark.sql.Row
//    val training = spark.createDataFrame(Seq(
//      (1.0, Vectors.dense(0.0, 1.1, 0.1)),
//      (0.0, Vectors.dense(2.0, 1.0, -1.0)),
//      (0.0, Vectors.dense(2.0, 1.3, 1.0)),
//      (1.0, Vectors.dense(0.0, 1.2, -0.5))
//    )).toDF("label", "features")
//    val lr = new LogisticRegression()
//    println(s"LogisticRegression parameters:\n ${lr.explainParams()}\n")
//    lr.setMaxIter(10)
//      .setRegParam(0.01)
//    val model1 = lr.fit(training)
//    println(s"Model 1 was fit using parameters: ${model1.parent.extractParamMap}")
//    val paramMap = ParamMap(lr.maxIter -> 20)
//      .put(lr.maxIter, 30)
//      .put(lr.regParam -> 0.1, lr.threshold -> 0.55)
//    val paramMap2 = ParamMap(lr.probabilityCol -> "myProbability")
//    val paramMapCombined = paramMap ++ paramMap2
//    val model2 = lr.fit(training, paramMapCombined)
//    println(s"Model 2 was fit using parameters: ${model2.parent.extractParamMap}")
//    val test = spark.createDataFrame(Seq(
//      (1.0, Vectors.dense(-1.0, 1.5, 1.3)),
//      (0.0, Vectors.dense(3.0, 2.0, -0.1)),
//      (1.0, Vectors.dense(0.0, 2.2, -1.5))
//    )).toDF("label", "features")
//    model2.transform(test)
//      .select("features", "label", "myProbability", "prediction")
//      .collect()
//      .foreach { case Row(features: Vector, label: Double, prob: Vector, prediction: Double) =>
//        println(s"($features, $label) -> prob=$prob, prediction=$prediction")
//      }

//    // 13.
//    import org.apache.spark.ml.{Pipeline, PipelineModel}
//    import org.apache.spark.ml.classification.LogisticRegression
//    import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
//    import org.apache.spark.ml.linalg.Vector
//    import org.apache.spark.sql.Row
//    val training = spark.createDataFrame(Seq(
//      (0L, "a b c d e spark", 1.0),
//      (1L, "b d", 0.0),
//      (2L, "spark f g h", 1.0),
//      (3L, "hadoop mapreduce", 0.0)
//    )).toDF("id", "text", "label")
//    val tokenizer = new Tokenizer()
//      .setInputCol("text")
//      .setOutputCol("words")
//    val hashingTF = new HashingTF()
//      .setNumFeatures(1000)
//      .setInputCol(tokenizer.getOutputCol)
//      .setOutputCol("features")
//    val lr = new LogisticRegression()
//      .setMaxIter(10)
//      .setRegParam(0.001)
//    val pipeline = new Pipeline()
//      .setStages(Array(tokenizer, hashingTF, lr))
//    val model = pipeline.fit(training)
//    model.write.overwrite().save("spark-logistic-regression-model")
//    // We can also save this unfit pipeline to disk
//    pipeline.write.overwrite().save("/tmp/unfit-lr-model")
//    val sameModel = PipelineModel.load("spark-logistic-regression-model")
//    val test = spark.createDataFrame(Seq(
//      (4L, "spark i j k"),
//      (5L, "l m n"),
//      (6L, "spark hadoop spark"),
//      (7L, "apache hadoop")
//    )).toDF("id", "text")
//    model.transform(test)
//      .select("id", "text", "probability", "prediction")
//      .collect()
//      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
//        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
//      }

//    // 14.
//    import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
//    val sentenceData = spark.createDataFrame(Seq(
//      (0.0, "Hi I heard about Spark"),
//      (0.0, "I wish Java could use case classes"),
//      (1.0, "Logistic regression models are neat")
//    )).toDF("label", "sentence")
//    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
//    val wordsData = tokenizer.transform(sentenceData)
//    val hashingTF = new HashingTF()
//      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
//    val featurizedData = hashingTF.transform(wordsData)
//    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
//    val idfModel = idf.fit(featurizedData)
//    val rescaledData = idfModel.transform(featurizedData)
//    rescaledData.select("label", "features").show()

//    // 15.
//    import org.apache.spark.ml.feature.Word2Vec
//    import org.apache.spark.ml.linalg.Vector
//    import org.apache.spark.sql.Row
//    val documentDF = spark.createDataFrame(Seq(
//      "Hi I heard about Spark".split(" "),
//      "I wish Java could use case classes".split(" "),
//      "Logistic regression models are neat".split(" ")
//    ).map(Tuple1.apply)).toDF("text")
//    val word2Vec = new Word2Vec()
//      .setInputCol("text")
//      .setOutputCol("result")
//      .setVectorSize(3)
//      .setMinCount(0)
//    val model = word2Vec.fit(documentDF)
//    val result = model.transform(documentDF)
//    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
//      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }

//    // 16.
//    import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
//    val df = spark.createDataFrame(Seq(
//      (0, Array("a", "b", "c")),
//      (1, Array("a", "b", "b", "c", "a"))
//    )).toDF("id", "words")
//    val cvModel: CountVectorizerModel = new CountVectorizer()
//      .setInputCol("words")
//      .setOutputCol("features")
//      .setVocabSize(3)
//      .setMinDF(2)
//      .fit(df)
//    val cvm = new CountVectorizerModel(Array("a", "b", "c"))
//      .setInputCol("words")
//      .setOutputCol("features")
//    cvModel.transform(df).show(false)

//    // 17.
//    import org.apache.spark.ml.feature.FeatureHasher
//    val dataset = spark.createDataFrame(Seq(
//      (2.2, true, "1", "foo"),
//      (3.3, false, "2", "bar"),
//      (4.4, false, "3", "baz"),
//      (5.5, false, "4", "foo")
//    )).toDF("real", "bool", "stringNum", "string")
//    val hasher = new FeatureHasher()
//      .setInputCols("real", "bool", "stringNum", "string")
//      .setOutputCol("features")
//    val featurized = hasher.transform(dataset)
//    featurized.show(false)

//    // 18.
//    import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
//    import org.apache.spark.sql.SparkSession
//    import org.apache.spark.sql.functions._
//    val sentenceDataFrame = spark.createDataFrame(Seq(
//      (0, "Hi I heard about Spark"),
//      (1, "I wish Java could use case classes"),
//      (2, "Logistic,regression,models,are,neat")
//    )).toDF("id", "sentence")
//    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
//    val regexTokenizer = new RegexTokenizer()
//      .setInputCol("sentence")
//      .setOutputCol("words")
//      .setPattern("\\W") // alternatively .setPattern("\\w+").setGaps(false)
//    val countTokens = udf { (words: Seq[String]) => words.length }
//    val tokenized = tokenizer.transform(sentenceDataFrame)
//    tokenized.select("sentence", "words")
//      .withColumn("tokens", countTokens(col("words"))).show(false)
//    val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
//    regexTokenized.select("sentence", "words")
//      .withColumn("tokens", countTokens(col("words"))).show(false)

//    // 19.
//    import org.apache.spark.ml.feature.StopWordsRemover
//    val remover = new StopWordsRemover()
//      .setInputCol("raw")
//      .setOutputCol("filtered")
//    val dataSet = spark.createDataFrame(Seq(
//      (0, Seq("I", "saw", "the", "red", "balloon")),
//      (1, Seq("Mary", "had", "a", "little", "lamb"))
//    )).toDF("id", "raw")
//    remover.transform(dataSet).show(false)

//    // 20.
//    import org.apache.spark.ml.feature.NGram
//    val wordDataFrame = spark.createDataFrame(Seq(
//      (0, Array("Hi", "I", "heard", "about", "Spark")),
//      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
//      (2, Array("Logistic", "regression", "models", "are", "neat"))
//    )).toDF("id", "words")
//    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")
//    val ngramDataFrame = ngram.transform(wordDataFrame)
//    ngramDataFrame.select("ngrams").show(false)

//    // 21.
//    import org.apache.spark.ml.feature.Binarizer
//    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
//    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")
//    val binarizer: Binarizer = new Binarizer()
//      .setInputCol("feature")
//      .setOutputCol("binarized_feature")
//      .setThreshold(0.5)
//    val binarizedDataFrame = binarizer.transform(dataFrame)
//    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
//    binarizedDataFrame.show()

//    // 22.
//    import org.apache.spark.ml.feature.PCA
//    import org.apache.spark.ml.linalg.Vectors
//    val data = Array(
//      Vectors.sparse(5, Seq((1, 1.0), (3, 7.0))),
//      Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0),
//      Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0)
//    )
//    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
//    val pca = new PCA()
//      .setInputCol("features")
//      .setOutputCol("pcaFeatures")
//      .setK(3)
//      .fit(df)
//    val result = pca.transform(df).select("pcaFeatures")
//    result.show(false)

//    // 23.
//    import org.apache.spark.ml.feature.PolynomialExpansion
//    import org.apache.spark.ml.linalg.Vectors
//    val data = Array(
//      Vectors.dense(2.0, 1.0),
//      Vectors.dense(0.0, 0.0),
//      Vectors.dense(3.0, -1.0)
//    )
//    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
//    val polyExpansion = new PolynomialExpansion()
//      .setInputCol("features")
//      .setOutputCol("polyFeatures")
//      .setDegree(3)
//    val polyDF = polyExpansion.transform(df)
//    polyDF.show(false)

//    // 24.
//    import org.apache.spark.ml.feature.DCT
//    import org.apache.spark.ml.linalg.Vectors
//    val data = Seq(
//      Vectors.dense(0.0, 1.0, -2.0, 3.0),
//      Vectors.dense(-1.0, 2.0, 4.0, -7.0),
//      Vectors.dense(14.0, -2.0, -5.0, 1.0))
//    val df = spark.createDataFrame(data.map(Tuple1.apply)).toDF("features")
//    val dct = new DCT()
//      .setInputCol("features")
//      .setOutputCol("featuresDCT")
//      .setInverse(false)
//    val dctDf = dct.transform(df)
//    dctDf.select("featuresDCT").show(false)

//    // 25.
//    import org.apache.spark.ml.feature.StringIndexer
//    val df = spark.createDataFrame(
//      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
//    ).toDF("id", "category")
//    val indexer = new StringIndexer()
//      .setInputCol("category")
//      .setOutputCol("categoryIndex")
//    val indexed = indexer.fit(df).transform(df)
//    indexed.show()

//    // 26.
//    import org.apache.spark.ml.attribute.Attribute
//    import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
//    val df = spark.createDataFrame(Seq(
//      (0, "a"),
//      (1, "b"),
//      (2, "c"),
//      (3, "a"),
//      (4, "a"),
//      (5, "c")
//    )).toDF("id", "category")
//    val indexer = new StringIndexer()
//      .setInputCol("category")
//      .setOutputCol("categoryIndex")
//      .fit(df)
//    val indexed = indexer.transform(df)
//    println(s"Transformed string column '${indexer.getInputCol}' " +
//      s"to indexed column '${indexer.getOutputCol}'")
//    indexed.show()
//    val inputColSchema = indexed.schema(indexer.getOutputCol)
//    println(s"StringIndexer will store labels in output column metadata: " +
//      s"${Attribute.fromStructField(inputColSchema).toString}\n")
//    val converter = new IndexToString()
//      .setInputCol("categoryIndex")
//      .setOutputCol("originalCategory")
//    val converted = converter.transform(indexed)
//    println(s"Transformed indexed column '${converter.getInputCol}' back to original string " +
//      s"column '${converter.getOutputCol}' using labels in metadata")
//    converted.select("id", "categoryIndex", "originalCategory").show()

//    // 27.
//    import org.apache.spark.ml.feature.OneHotEncoderEstimator
//    val df = spark.createDataFrame(Seq(
//      (0.0, 1.0),
//      (1.0, 0.0),
//      (2.0, 1.0),
//      (0.0, 2.0),
//      (0.0, 1.0),
//      (2.0, 0.0)
//    )).toDF("categoryIndex1", "categoryIndex2")
//    val encoder = new OneHotEncoderEstimator()
//      .setInputCols(Array("categoryIndex1", "categoryIndex2"))
//      .setOutputCols(Array("categoryVec1", "categoryVec2"))
//    val model = encoder.fit(df)
//    val encoded = model.transform(df)
//    encoded.show()

    // 28.
//    import org.apache.spark.ml.feature.VectorIndexer
//    val data = spark.read.format("libsvm").load("sample_libsvm_data.txt")
//    val indexer = new VectorIndexer()
//      .setInputCol("features")
//      .setOutputCol("indexed")
//      .setMaxCategories(10)
//    val indexerModel = indexer.fit(data)
//    val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
//    println(s"Chose ${categoricalFeatures.size} " +
//      s"categorical features: ${categoricalFeatures.mkString(", ")}")
//    // Create new column "indexed" with categorical values transformed to indices
//    val indexedData = indexerModel.transform(data)
//    indexedData.show(false)

    // 29.
    spark.createDataFrame(Seq(
      (0, 'a'),
      (1, 'b),
      (2, 'c),
      (3, 'd)
    )).toDF("index", "category").show()

//
  }


























}
