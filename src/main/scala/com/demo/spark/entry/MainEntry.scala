package com.demo.spark.entry

import com.demo.spark.session.DemoSparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger

object MainEntry {

  def main(args: Array[String]): Unit = {
    
    Logger.getLogger("org").setLevel(Level.OFF) 
    Logger.getLogger("akka").setLevel(Level.OFF)

    println("After Jenkins Intergration --------------->")
    println("Starting the entry point --------------->")

    val spark = DemoSparkSession.spark
    import spark.implicits._

    val data=spark.sparkContext.textFile("demo.txt")  
    val splitdata = data.flatMap(line => line.split(" "));  
    val mapdata = splitdata.map(word => (word,1));  
    val reducedata = mapdata.reduceByKey(_+_);  
    reducedata.collect().foreach(println)  

    println("Finishing the entry point --------------->")

  }

}