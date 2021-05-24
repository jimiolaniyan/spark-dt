import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import java.time.{LocalDateTime, ZonedDateTime}
import scala.util.{Random, Try}

object Nav {
  class UnbalancedPartitioner(override val numPartitions: Int) extends Partitioner {
    var x = 0
    override def getPartition(key: Any): Int = key match {
      case s: String =>
        val id:Int = Try(s.toInt).getOrElse(-1)
        if (id > -1) {
          if (id % numPartitions == 0) {
            // change here to create more unbalanced partitions
            Random.nextInt(2)
          } else
//          id % numPartitions
          Random.nextInt(numPartitions)
        } else 0
    }
  }

  object Timelog {
    val timers = scala.collection.mutable.Map.empty[String, Long]

    def timer(t:String) = {
      if (timers contains t) {
        val output = s"$t: ${(System.nanoTime() - timers(t)) / 1000 / 1000} " +
          s"ms (${(System.nanoTime() - timers(t)).round / 1000 / 1000/1000.toFloat} sec) @ ${LocalDateTime.now()}"
        println(output)
        println("")
      } else {
        println(t + ": started at: " + LocalDateTime.now())
        timers(t) = System.nanoTime()
      }
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
    Timelog.timer("TOTAL TIME")
    val conf = new SparkConf(loadDefaults = true)
      .setAppName("CLIWOC")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val partitions = sys.env("PARTS")
    Timelog.timer("[T1] READ FILE")
    val wholeFile = sc.textFile("data/CLIWOC15.csv", minPartitions = partitions.toInt)
    Timelog.timer("[T1] READ FILE")
//    val firstLine = wholeFile.filter(_.contains("RecID")).collect()(0).filterNot(_ == '"').split(',')
    // filter out the first line from the initial RDD
//    val entries_tmp = wholeFile.filter(!_.contains("RecID"))

    // split each line into key value pairs
    Timelog.timer("[T2] MAP 1")
    val entries = wholeFile.map(x => {
      val y = x.split(',')
      (y(0),  y.drop(0))
    })
    Timelog.timer("[T2] MAP 1")

//    Timelog.timer("[T2] MAP 1")
//    val entries = wholeFile.map(x => x.split(','))
//    Timelog.timer("[T2] MAP 1")

    Timelog.timer("[T3] COUNT 1")
    println("Count " + entries.count())
    Timelog.timer("[T3] COUNT 1")

    Timelog.timer("[T4] PARTITION")
    val rdd1 = entries.partitionBy(new UnbalancedPartitioner(partitions.toInt))
    Timelog.timer("[T4] PARTITION")

    Timelog.timer("[T5] COUNT 2 (PARTITION)")
    println("Count " + rdd1.count())
    Timelog.timer("[T5] COUNT 2 (PARTITION)")

    Timelog.timer("[T6] MAP 2 (PARTITION)")
    val data = rdd1.map(x => x)
    Timelog.timer("[T6] MAP 2 (PARTITION)")

    //    Timelog.timer("[T4] MAP 2")
//    val data = wholeFile.map(x => x.split(','))
//    Timelog.timer("[T4] MAP 2")

    Timelog.timer("[T7] COUNT 3 (PARTITION)")
    println("Count " + data.count())
    Timelog.timer("[T7] COUNT 3 (PARTITION)")

    Timelog.timer("TOTAL TIME")
  }
}
