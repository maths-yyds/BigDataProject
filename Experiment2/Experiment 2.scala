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
val warcfile = s"file:///opt/hadoop/rubigdata/CC-MAIN-20210411085610-20210411115610-00465.warc.gz"
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

//Codes for filtering, result in Result 3.txt on GitHub
val wb = warcs.map{ wr => wr._2.getRecord().getHttpStringBody()}.
                map{ wb => {
                val d = Jsoup.parse(wb)
                val t = d.title()
                val links = d.select("a").asScala
                links.map(l => t).
                filter(x => x.contains("250")).
                toIterator
                }
            }.flatMap(identity)
wb.distinct().take(600).foreach(println)

//Intermediate step, result in Result 4.txt on GitHub
val wb = warcs.map{ wr => wr._2.getRecord().getHttpStringBody()}.
                map{ wb => {
                    val d = Jsoup.parse(wb)
                    val t = d.title()
                    val links = d.select("a").asScala
                    links.map(l => (t, l.text,l.attr("href"),l.attr("title"))).
                    filter(x => x._1 == "IMDb Top 250 - IMDb").
                    filter(x => x._2 != "").
                    filter(x => x._3.contains("/title/tt")).
                    filter(x => x._4 != "").
                    toIterator
                    }
                }.flatMap(identity)
println(wb.count)
wb.take(250).foreach(println)

//Codes for counting directors, result in Result 5.txt on GitHub
val dir = wb.map(x => x._4.split(", ")(0)).map(x => (x,1)).reduceByKey((A,B) => A+B).map(x => (x._2, x._1)).sortByKey(false)
dir.take(250).foreach(println)

//Codes for analyzing movie ratings, result in Result 6.txt on GitHub
val rating = warcs.map{wr => wr._2.getRecord().getHttpStringBody()}.
                map{wb => ({
                    val d = Jsoup.parse(wb)
                    val t = d.title()
                    val links = d.select("td").asScala
                    links.filter(_.attr("class") == "titleColumn").
                    map(l => (t,l.text)).
                    filter(x => x._1 == "IMDb Top 250 - IMDb").
                    map(x => x._2).toIterator
                },
                {
                    val d = Jsoup.parse(wb)
                    val t = d.title()
                    val links = d.select("td strong").asScala
                    links.map(l => (t,l.attr("title"))).
                    filter(x => x._1 == "IMDb Top 250 - IMDb").
                    map(x => x._2).toIterator
                }
            )
        }.flatMap(x => x._1.zip(x._2))
rating.take(250).foreach(println)
  }
}
