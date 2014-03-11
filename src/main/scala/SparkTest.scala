import java.util
import org.apache.spark.mllib.clustering._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable
import scala.math._

object SparkTest {
  def main(args: Array[String]) {

    val sc = new SparkContext("local", "HotNewsSparkTest", "/root/workspace/bigdata/spark-0.8.0-incubating",
      List("target/scala-2.9.3/hotnewsspark_2.9.3-1.0.jar"))

    //    val data = sc.textFile("iris.txt").map(line => line.split(' ').map(_.toDouble)).cache()
    //
    //    val model = KMeans.train(data, 3, 20, 10)
    //
    //    val result  = data.map(model.predict)
    //
    //    result.saveAsTextFile("reslut2")
    val newsFeatureExtract = new NewsFeatureExtract
    //获取新闻到一个Map中，key是新闻的Title，value是新闻的Content
    val newsMap = newsFeatureExtract.readNews

    import scala.collection.JavaConverters._
    val myScalaMap = newsMap.asScala

    val newsRdd = sc.parallelize(myScalaMap.toList)

    // RDD[(String, String)]----> RDD[(String, Array[String])] 将新闻的内容进行分词
    val cuttedNews = newsRdd.map{ news =>
      val javaListString: util.List[String] = newsFeatureExtract.ansjCutWord(news._2)
      (news._1, javaListString.asScala.toArray)
    }

    //RDD[(String, Array[String])]----> RDD[((String, String), Double)]
    //统计词频，生成RDD[((标题，单词)，这个词在整篇文章中出现的频率)]
    val wordFrequency = cuttedNews.flatMap{ cuttedNew =>
      val title = cuttedNew._1
      val bagOfWords = cuttedNew._2
      val wordsLength = bagOfWords.length
      for(word <- bagOfWords) yield ((title, word, wordsLength), 1)
    }.reduceByKey((a,b) => a + b).map{temp =>
      val title = temp._1._1
      val word = temp._1._2
      val totalLength = temp._1._3
      val count = temp._2
      ((title,word), count.toDouble/totalLength)
    }

    //RDD[(String, Array[String])]----> RDD[((String, String), Double)]
    //计算idf，生成RDD[(标题，单词)，idf]
    val totalNewsNumber =  myScalaMap.size
    val idf = cuttedNews.flatMap{ cuttedNew =>
      val title = cuttedNew._1
      val bagOfWords = cuttedNew._2
      val distinctList = bagOfWords.toList.distinct
      for(word <- distinctList) yield {(word, title) }
    }.groupByKey().flatMap{ item =>
      val word = item._1
      val titleList = item._2
      val titleListNumber = titleList.length
      val idf = log10((1+totalNewsNumber)/titleListNumber)
      for(title <- titleList) yield {
        ((title,word), idf )
      }

    }

    //生成RDD[(String, Seq[(String, Double)])]
    //计算每篇文档中每个词的分数，按照分数从高到低排列，取出分数最高的前十个。如果这个词出现在标题中，则相应的增加这个词的权重
    val scoreRdd = wordFrequency.join(idf).map{ item =>
          val title = item._1._1
          val word = item._1._2
          val tf = item._2._1
          val idf = item._2._2
          var score = tf * idf * 5
          val titleWordList = newsFeatureExtract.ansjCutWord(title).asScala.toArray
          for(titleWord <- titleWordList){
            if(titleWord == word){
              score *= 2
            }
      }
      ((title, (word, score)))
    }.groupByKey().map{ item =>
      val title = item._1
      val wordScoreList: Seq[(String, Double)] = item._2
      var accu = 0.0
      for(wordScore <- wordScoreList){
        accu += wordScore._2 * wordScore._2
      }
      wordScoreList.map{ (wordScore: (String, Double)) =>
        (wordScore._1, wordScore._2/accu)

      }

      (title, wordScoreList.sortBy(_._2).reverse.take(10))

    }.cache()


    val features = scoreRdd.collect()
    println(features.mkString("\n"))
    val featureSet = new mutable.HashSet[String]()
    features.foreach{ item =>
     val featureList = item._2
     for(feature <- featureList){
       featureSet.add(feature._1)
     }
   }
   println("--------------------------------------------------")
    println(featureSet)
    println("--------------------------------------------------")

    val finalFeatureRdd = scoreRdd.map{ item =>
     val featureList = item._2
     val featureStringList = featureList.map(_._1)
     val absentFeatures = featureSet.filter{ feature =>
       !featureStringList.contains(feature)
     }
     val buffer = featureList.toBuffer

     for(feature <- absentFeatures){
       buffer ++= Seq((feature, 0.0))
     }
     (item._1, buffer.sortBy(_._1).toList)
   }

    println("------------------------------------------------------------------------------------")
    val featuresFinal = finalFeatureRdd.collect()

    println(featuresFinal.mkString("\n"))

    featuresFinal.foreach{ item =>
      if(item._2.length != featureSet.size){
        println("error!!!! Length not equle")
      }

    }


 }


}
