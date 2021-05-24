package optiSeg
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import org.apache.spark.scheduler._
import org.apache.log4j.LogManager
import org.apache.spark._
import System.nanoTime
import scala.collection.immutable.TreeMap
import scala.io.Source._
import org.apache.spark.Partitioner
import java.io._
import scala.io.Source
import scala.reflect.io.Directory
import scala.util.parsing.combinator.syntactical
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.util.SizeEstimator
import java.nio.ByteBuffer
import java.util.Arrays

/******************************************* TIME ANALYSIS *****************************************/
object Timelog {
	val timers = scala.collection.mutable.Map.empty[String, Long]
			val timers_t = scala.collection.mutable.Map.empty[String, Long]

					def timer(t:String) = {
							if (timers contains t) {
								val output = s"$t: ${(System.nanoTime() - timers(t)) / 1000 / 1000} ms (${(System.nanoTime() - timers(t)).round / 1000 / 1000/1000.toFloat} sec)"
								println("")
								println(output)
							}
							else timers(t) = System.nanoTime()
	}

}
/************************************************************************************************/
object optiSeg {
	def VectorToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
		val p = new java.io.PrintWriter(f)
				try { op(p) } finally { p.close() }
	}

	def deleteRecursively(file: File): Unit = {
			if (file.isDirectory)
				file.listFiles.foreach(deleteRecursively)
				if (file.exists && !file.delete)
					throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
	}

	/******************************************* MAIN ***********************************************/
	def main(args: Array[String]): Unit={
			Timelog.timer("TOTAL TIME")
			//val conf = new SparkConf().setAppName("OptiSeg_spark").setMaster("spark://dahu-13.grenoble.grid5000.fr:7077")
                        val conf = new SparkConf().setAppName("OptiSeg_spark").setMaster("local")
			val sc= new SparkContext(conf)
			sc.setLogLevel("ERROR")

			/*Passing arguments*/
			if (args.length<1){
				println("!!! missing Matrix Dimension...")
				System.exit(1)
			}

			/** the number of partitions we are going to use to parallelize the work */
			val partitionNB =96 //cores*2=240
			
			/** nbE is the number of elements we are going to process */
			val nbE=args(0).toInt
			
			/** since it exists only on the driver, we can use a broadcast variable to make it visible to all nodes */
			val nbE_bcast = sc.broadcast(nbE).value
			val mid = nbE_bcast / 2
			
			//val halfN= nbE_bcast/2
			//val halfN_bcast = sc.broadcast(halfN).value
			//val mid_bcast = sc.broadcast(mid).value
			
			/** Read dataset file */
			Timelog.timer("[T1]READ FILE AND EXTRACT VALUES")

			val lines = sc.textFile("./data/REF_15571/dataset_f7x_02_F7XBleedAnalysis_30.csv",1)

			/** out of the file, we extract nbE_bcast values */
			val values = lines.map(_.split(";")(1).toDouble).take(nbE_bcast)
			
			// TR: it might make sense to broadcast the values to improve the performance
			
			Timelog.timer("[T1]READ FILE AND EXTRACT VALUES")
			Timelog.timer("[T2]INITIAL RDD")
			  
			/** we are now going to build the triangular matrix */
			
			/** The strategy to balance the work is to create N/2 entries,
			 *  and to group together the first and the last row, 2nd with N-1, etc.
			 */
			
			/** We start be creating a list of indexes from 0 to N/2 */
			val idx = (0 until mid).toArray
			//println(idx)
			
			/** we create a RDD that includes idx entries and that is partitioned */
			/** each entry is an array 
			 *  For an index K:
			 *  	- The first entry in the table includes the K first entries of the input data
			 *  	- the second entry in the table includes the N-K first entries if the input data 
			 */
			val N = nbE_bcast -1
			val SEQ = sc.parallelize(idx,partitionNB).map{
				idx =>
				if (idx < N - idx) //&& i%2==0){  // dirty condition ... to be updated
				{
					var array = new Array[(Int,Array[Double])](2)
					val x = values.drop(idx)
					//println("x="+x.toList)
					val y = values.drop(N - idx)
					//  println("y="+y.toList)
					array(0) = (idx,x)
					array(1) = (N-idx,y)
					array
				}
				// else if (i > N - i && i%2==0){
				//   var array = new Array[(Int,Array[Double])](2)
				//   val x = values.drop(i)
				//   //println("x="+x.toList)
				//   val y = values.drop(N - i)
				// //  println("y="+y.toList)
				//   array(0) = (i,x)
				//   array(1) = (N-i,y)
				//   array
				// }
			}
			
			// to force transformation to be applied
			SEQ.count()
			Timelog.timer("[T2]INITIAL RDD")

			/** Building the cost matrix */
			Timelog.timer("[T3]BUILD MATRIX")
			//val tmp = sc.parallelize(SEQ,partitionNB)
			
			/** For each row of the matrix, we can apply the cost function of the algorithm
			 *  For each entry in the RDD, we actually execute the code on the 2 entries of the array
			 */
			
			/** each element in the RDD correspond to the computed costs for each element in one row
			 *  The elements are stored in the form of a tuple: (column_index, (row_index, lse))
			 */
			
			val result = SEQ.map{case Array((r1: Int,y1: Array[Double]),(r2: Int,y2: Array[Double])) =>
			var a1,a2: Double =0
			var b1,b2: Double =0
			var lse1,lse2: Double =0
			var sxy1,sxy2: Double =0
			var sx1,sx2: Double =0
			var sx12,sx22: Double =0
			var sy1,sy2: Double =0
			var x1,x2: Int =0
			var d1,d2 : Int =0
			var idx1,idx2: Int =0
			var i1: Int = r1
			var j1: Int = y1.size
			//println("j1"+j1)
			//println("j2"+j2)
			var array1 = new Array[(Int,(Int,Double))](j1)
			var J:Int = 0
			for(J<- i1 until nbE_bcast){
				x1 = J
						d1 = (J-i1+1)

						sx1 = sx1 + x1
						var x12= (Math.pow(x1,2))
						sx12 = sx12 + x12
						var ts_1 = y1(d1-1)
						sy1 = sy1 + ts_1
						sxy1 = sxy1 + x1 * ts_1
						if (d1 > 2){
							var t1= (Math.pow(sx1,2))
									a1 = (d1*sxy1 - (sx1*sy1))/(d1*sx12 - t1)
									b1 = (sy1 - (a1*sx1))/d1
									lse1 = 0
									var ii1:Int=i1
									for(k <- 0 until d1){
										var p1 = (Math.pow(y1(k) /*._1*/-a1*ii1 -b1,2))
												lse1 = (lse1 + p1)
												ii1=ii1 + 1
									}
							array1(idx1)=(J,(i1,lse1))
						}
						else{
							array1(idx1)=(J,(i1,0.0))
						}
				idx1 = idx1+1
			} // END of For1


			var j2: Int = y2.size
					var i2: Int = r2
					var array2 = new Array[(Int,(Int,Double))](j2)

					//J= 0
					for(j2<- i2 until nbE_bcast){
						x2 = j2
								d2 = (j2-i2+1)
								sx2 = sx2 + x2
								var x22= (Math.pow(x2,2))
								sx22 = sx22 + x22
								var ts_2 = y2(d2-1)
								sy2 = sy2 + ts_2
								sxy2 = sxy2 + x2 * ts_2
								if (d2 > 2){
									var t2= (Math.pow(sx2,2))
											a2 = (d2*sxy2 - (sx2*sy2))/(d2*sx22 - t2)
											b2 = (sy2 - (a2*sx2))/d2
											lse2 = 0
											var ii2:Int=i2
											for(k <- 0 until d2){
												var p2 = (Math.pow(y2(k) /*._1*/-a2*ii2 -b2,2))
														lse2 = (lse2 + p2)
														ii2=ii2 + 1
											}
									array2(idx2)=(j2,(i2,lse2))
								}
								else{
									array2(idx2)=(j2,(i2,0.0))
								}
						idx2 = idx2+1
					} // END of For2
				var res =  new Array[Array[(Int,(Int,Double))]](2)
						res(0)= array1
						res(1)= array2
						res
			} //END of Map
			
			// To force transformation to be executed. It can be removed if absolute performance should be checked
			result.count()
			
			//result.foreach(println)

			Timelog.timer("[T3]BUILD MATRIX")

			/** Now that we have the matrix that is built, we are going to make all elements
			 *  independent so that we can then group them by column
			 *  Because of the way we stored the elements, each entry has its column_index has key
			 */
			Timelog.timer("[T4]FLATTEN PROCESS")
			val flatten = result.flatMap{x=> x.toSeq.flatten}
			
			// forces transformation to be executed
			flatten.count()

			Timelog.timer("[T4]FLATTEN PROCESS")

			Timelog.timer("[T5]GROUP BY COLUMN 1")

			/** we group all the elements by column index */
			val col_ = flatten.groupByKey().map{x=> (x._1,x._2.toArray)}.cache()
			col_.count()
			Timelog.timer("[T5]GROUP BY COLUMN 1")

			Timelog.timer("[T6]CHANGEPOINT VECTOR")
			/** finally we compute the vector of changepoints according to the penalty */
			val j= 0
			val penal = 100
			var mincost=  new Array[Double](nbE)
			var minpath=  new Array[Int](nbE)
			var currmin:Double=0
			var currIdx:Int=0
			var currcost:Double=0
			mincost.update(0,0.0)
			minpath.update(0,0)
			
			var c:Int=0
			/** To run the computation, we are collecting columns on the driver block by block */
			/** when the dataset is large, 1000 blocks are created */
			var blk:Int=1
			if (nbE <1000){blk = nbE}
			else{c = nbE/1000;blk = nbE/c}
			var ct:Long= 0
			Timelog.timer("[T*]First Part")
			// TR: Not very clear what is the purpose of this line (why would there be elements with index more that nbE)
			val rdd1  = col_.filter(x=> x._1 < nbE).cache()
			for (k<-0 until nbE by blk){
			  /** we collect a block of data */
				var data = rdd1.filterByRange(k, k+blk-1).collect()//.map{case(x,y)=>(x,y.getInt[4].toInt, y.getDouble[8].toDouble)}.collect()
				/** we compute the corresponding change poitns */
				compute(data)
			}
			Timelog.timer("[T*]First Part")
			/*
    Timelog.timer("[T*]Second Part")
    rdd1.unpersist()

    val rdd2 = col_.filter(x=> x._1 >= mid).cache()
    col_.unpersist()
    for (k<-mid until nbE by blk){
     var data1 = rdd2.filterByRange(k, k+blk-1).collect()
     //data.persist(DISK_ONLY)
//     compute(data1)
  }
			 */

			def compute[T](arr: Array[(Int,Array[(Int,Double)])]):Unit={
					var m= arr.toMap
							//  arr.foreach(println)
							var sorted = TreeMap(m.toArray:_*).toArray
							var s = sorted.head._1
							val e = sorted.last._1
							var j:Int = s
							var idx: Int = 0
							do{
								if (j!=0) {
									currIdx=j
											currmin=mincost.apply(j-1)+penal
											for(t<- 0 to j){
												var i = sorted.apply(idx)._2(t)._1
														if(i!=0 && i!=j){
															currcost = sorted.apply(idx)._2(t)._2 + mincost.apply(i-1)+penal
																	if(currmin >  currcost) {
																		currmin=currcost
																				currIdx=i
																	}
														}
											}
									mincost.update(j,currmin)
									minpath.update(j,currIdx)
								}
								j = j+1
										idx = idx+1
							}while(j < e+1 )
			}
			minpath = minpath.patch(0,Nil,1)
					VectorToFile(new File("output")){p=> minpath.foreach(p.println)}
			Timelog.timer("[T6]CHANGEPOINT VECTOR")

			Timelog.timer("TOTAL TIME")
			sc.stop()
	} /*END MAIN*/
} /*END OBJECT*/
