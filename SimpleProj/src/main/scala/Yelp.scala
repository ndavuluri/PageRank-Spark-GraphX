import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.jsoup.Jsoup
import org.jsoup.nodes.Element

import scala.collection.mutable.ListBuffer

object Yelp {
  def main(args: Array[String]) {
   if (args.length < 3) {
      System.err.println(
        "Usage: PageRank <inputFile> <outputFile> <No of iterations>")
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName("PageRank Application")
    val sc = new SparkContext(conf)

    val PhotoFile = args(0) 
    val PhotoData = sc.textFile(PhotoFile)
    val LabelFile = args(1)
    val LabelData = sc.textFile(LabelFile) 
    

    val photos = PhotoData.map{line =>
          val filefields = line.split(",")
          if(filefields.length > 1){
               (filefields(1),filefields(0))}
          else{
               ("unspecifed", filefields(0))
          }
     }.cache()

    val labels = LabelData.map{line =>
          val filefields = line.split(",")
          if(filefields.length > 1){
             (filefields(0),filefields(1))}
          else{
             ("unspecifed", filefields(0))
          }
    }.cache()

      val  contribs = photos.join(labels).values        
      val output = contribs.saveAsTextFile(args(2))
      sc.stop()

  }
}
