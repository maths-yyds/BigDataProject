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
//Operations on 60/640 WARC files
val warcfile = s"hdfs:///single-warc-segment/CC-MAIN-20210410105831-20210410135831-00[0-5][0-9]0.warc.gz"
//Operations on all 640 WARC files, will run this code if time permits
//val warcfile = s"hdfs:///single-warc-segment/*.warc.gz"

val sparkConf = new SparkConf()
                    .setAppName("RUBigDataApp")
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .registerKryoClasses(Array(classOf[WarcRecord]))
implicit val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
val sc = sparkSession.sparkContext
val warcs = sc.newAPIHadoopFile(
            warcfile,
            classOf[WarcGzInputFormat],             // InputFormat
            classOf[NullWritable],                  // Key
            classOf[WarcWritable]                   // Value
    )

//Codes for counting imcoming links, result in Result 7.txt on GitHub
val link = warcs.map{ wr => wr._2.getRecord().getHttpStringBody()}.
                map{ wb => {
                    val d = Jsoup.parse(wb)
                    val t = d.title()
                    val links = d.select("a").asScala
                    links.map(l => (t,l.attr("href"))).
                    map(x => 
                    try {new URI(x._2).getHost().replaceFirst("www.","")} 
                    catch {case ex: Exception => null}).
                    filter(x => x!=null).
                    map(x => (x,1)).toIterator
                    }
                }.flatMap(identity)         
link.reduceByKey((A,B) => A+B).map(x => (x._2, x._1)).sortByKey(false).take(200).foreach(println)
  }
}
