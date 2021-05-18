import org.apache.spark.{SparkConf, SparkContext}

object Navigation {

  // Finds out the index of "name" in the array firstLine
  // returns -1 if it cannot find it
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
    // start spark with 1 worker thread
    val conf = new SparkConf(loadDefaults = true)
      .setAppName("CLIWOC")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    // read the input file into an RDD[String]
    val wholeFile = sc.textFile("data/CLIWOC15.csv")

    // The first line of the file defines the name of each column in the csv file
    // We store it as an array in the driver program
    val firstLine = wholeFile.filter(_.contains("RecID")).collect()(0).filterNot(_ == '"').split(',')

    // filter out the first line from the initial RDD
    val entries_tmp = wholeFile.filter(!_.contains("RecID"))

    // split each line into an array of items
    val entries = entries_tmp.map(x => x.split(','))

    // keep the RDD in memory
    entries.cache()

    // ##### Create an RDD that contains all nationalities observed in the
    // ##### different entries
    //    println("count: " + entries.count())
    // Information about the nationality is provided in the column named "Nationality"


    // First find the index of the column corresponding to the "Nationality"
    val nation_idx = findCol(firstLine, "Nationality")
    val year_idx = findCol(firstLine, "Year")
    val voyage_idx = findCol(firstLine, "VoyageFrom")


    println("Nationality corresponds to columns: " + nation_idx.toString)
    println("Year corresponds to columns: " + year_idx.toString)

    // Use 'map   ' to create a RDD with all nationalities and 'distinct' to remove duplicates
    val nationalities = entries.map(x => clean(x(nation_idx))).distinct()
    // Display the 5 first nationalities
    println("A few examples of nationalities:")
    nationalities.sortBy(x => x).take(5).foreach(println)

    val years = entries.map(x => (clean(x(year_idx)), 1)).reduceByKey(_ + _)

    println("\nObservations were made over " + years.count() + " years")
    println("Years min: " + years.min()._1)
    println("Years max: " + years.max()._1)

    val years_count = years.map(x => (x._2, x._1))

    val min_count = years_count.min()
    val max_count = years_count.max()


    println("year: " + min_count._2 + " had the min with: " + min_count._1 + " while year: " + max_count._2
      + " had the max count with: " + max_count._1)

    val deps = entries.map(e =>  clean(e(voyage_idx))).distinct()
    println("departure places distinct method: " + deps.count())

//    val deps2 = entries.map(e =>  (clean(e(voyage_idx)), 1)).reduceByKey(_ + _)
//    println("departure places reduceByKey method: " + deps2.count())

    deps.takeOrdered(10)(Ordering[String].reverse).foreach(println)
    println("Took: " + (System.nanoTime - t1) / 1e9d)

    //    val l = entries.map(x => (clean(x(year_idx)), x)).countByValue()

    //    l.take(5).foreach(println)
    // prevent the program from   terminating immediately
    //println("Press Enter to continue...")
  //  scala.io.StdIn.readLine()
    sc.stop()
    sys.exit(0)
  }
}
