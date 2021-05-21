import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.util.{Random, Try}

object Nav {
  class UnbalancedPartitioner(override val numPartitions: Int) extends Partitioner {
    var x = 0
    override def getPartition(key: Any): Int = key match {
      case s: String =>
        val id:Int = Try(s.toInt).getOrElse(-1)
        if (id > -1) {
          if (id % numPartitions == 0) {
            Random.nextInt(2)
          } else
          Random.nextInt(numPartitions)
        } else 0
    }
  }

  def findCol(firstLine: Array[String], name: String): Int = {
    if (firstLine.contains(name)) {
      firstLine.indexOf(name)
    }
    else {
      -1
    }
  }

  def clean(str: String): String = {
    str.replaceAll("^\"|\"$", "").trim()
  }

  def main(args: Array[String]): Unit = {
    val t1 = System.nanoTime
    val conf = new SparkConf(loadDefaults = true)
      .setAppName("CLIWOC")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val partitions = sys.env("PARTS")
    val wholeFile = sc.textFile("data/CLIWOC15.csv", minPartitions = partitions.toInt)
//    val firstLine = wholeFile.filter(_.contains("RecID")).collect()(0).filterNot(_ == '"').split(',')
    // filter out the first line from the initial RDD
//    val entries_tmp = wholeFile.filter(!_.contains("RecID"))

    // split each line into key value pairs
    val entries = wholeFile.map(x => {
      val y = x.split(',')
      (y(0),  y.drop(0))
    })

//    val entries = wholeFile.map(x => x.split(','))
    println("Count " + entries.count())

    val rdd1 = entries.partitionBy(new UnbalancedPartitioner(partitions.toInt))
    println("Count " + rdd1.count())

    val data = rdd1.map(x => x)
//    val data = wholeFile.map(x => x.split(','))
    println("Count " + data.count())

    println("Took: " + (System.nanoTime - t1) / 1e9d)
  }
}
