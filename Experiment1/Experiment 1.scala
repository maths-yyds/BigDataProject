package org.rubigdata

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document,Element}
import collection.JavaConverters._
import java.net.URI

object RUBigDataApp {
  def main(args: Array[String]) {
val warcfile = s"file:///opt/hadoop/rubigdata/movie.warc.gz"
val sparkConf = new SparkConf()
                    .setAppName("RUBigDataApp")
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .registerKryoClasses(Array(classOf[WarcRecord]))
implicit val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
val sc = sparkSession.sparkContext
val warcs = sc.newAPIHadoopFile(
            warcfile,
            classOf[WarcGzInputFormat],             
            classOf[NullWritable],                 
            classOf[WarcWritable]                  
    )

//Codes for counting directors, result in Result 1.txt on GitHub
val wb = warcs.map{wr => wr._2.getRecord().getHttpStringBody()}.
               map{wb => {
                val d = Jsoup.parse(wb)
                val t = d.title()
                val links = d.select("div.lister a").asScala
                links.map(l => (l.text,l.attr("title"),t)).filter(x => x._1 != "").toIterator
                }
            }.
            flatMap(identity)
val dir = wb.map(x => x._2.split(", ")(0)).map(x => (x,1)).reduceByKey((A,B) => A+B).map(x => (x._2, x._1)).sortByKey(false)
dir.take(250).foreach(y => println(y))

//Codes for analyzing movie ratings, result in Result 2.txt on GitHub
val rating = warcs.map{wr => wr._2.getRecord().getHttpStringBody()}.
                   map{wb => ({
                    val d = Jsoup.parse(wb)
                    val t = d.title()
                    val links = d.select("td").asScala
                    links.filter(_.attr("class") == "titleColumn").
                    map(l => l.text).toIterator
                    },
                    {
                    val d = Jsoup.parse(wb)
                    val t = d.title()
                    val links = d.select("td strong").asScala
                    links.map(l => l.attr("title")).toIterator
                    })
                }.flatMap(x => x._1.zip(x._2))
rating.take(250).foreach(println)
  }
}
