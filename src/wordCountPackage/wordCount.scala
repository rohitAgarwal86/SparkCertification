package wordCountPackage;

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

class wordCount {
  
  def main(args : Array[String])
  {
     val conf = new SparkConf()
     val sc = new SparkContext(conf)
     
     val inputFile = args(0)
     val outputFile =args(1)
     
     
     val input = sc.textFile(inputFile)
     
     val words = input.flatMap(x => x.split(","))
     
     val result = words.map(x => (x,1)).reduceByKey((x,y) => x+y)
     
     result.foreach(println)
     
     result.saveAsTextFile(outputFile)
     
     sc.stop()
     println("Stopped the spark context successfully")
     
  }
}