import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

import scala.collection.mutable.ListBuffer

object UniversityApp {
  def main(args: Array[String]) {
   if (args.length < 3) {
      System.err.println(
        "Usage: PageRank <inputFile> <outputFile> <No of iterations>")
      System.exit(-1)
    }

    val File = args(0) 
    val conf = new SparkConf().setAppName("University Application")
    val sc = new SparkContext(conf)

    val Data = sc.textFile(File)
    val lines = Data.flatMap(file => file.split("\n"))
    
    val links = Data.map{line =>
          val fields = line.split("\t")
          val xml = Jsoup.parse(fields(3).replace("\\n", "\n"))
          val targets = xml.getElementsByTag("target")
          var result = new ListBuffer[String]()
          for(target <- 0 to targets.size()-1){
            result += targets.get(target).text()
          }

      (fields(1),result.toList)
    }.cache()
    var ranks = links.mapValues(v => 1.0)

    val iters = args(2).toInt

    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }
      val university = sc.textFile(args(3))
      val univ = university.map(s => (s,1))
      val op = ranks.join(univ)
            val op2 = op.map(item => item).sortByKey(false,1).take(100)

      val temp = sc.parallelize(op2)
      temp.coalesce(1).saveAsTextFile(args(1))
	sc.stop()
  }
}
