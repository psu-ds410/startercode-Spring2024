//mandatory imports for spark rdds
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object WordCount {  // all code must be inside an object or class
    def main(args: Array[String]) = {  // this is the entry point to our code
        val sc = getSC()  // one function to get the sc variable
        val myrdd = getRDD(sc) // on function to get the rdd
        val counts = doWordCount(myrdd) // additional functions to do the computation
        saveit(counts, "myresults")  // save the rdd to your home directory in HDFS
    }

    def getSC() = { // get the spark context variable
        val conf = new SparkConf().setAppName("My Program Name") //change this
        val sc = SparkContext.getOrCreate(conf)
        sc
    }

    def getRDD(sc:SparkContext) = { // get the big data rdd
         sc.textFile("/datasets/wap")
    }

    def getTestRDD(sc: SparkContext) = { // create a small testing rdd
         val mylines = List("it was the best of times, wasn't it",
                            "it was the worst of times of all time")
         sc.parallelize(mylines, 3)

    }

    def doWordCount(input: RDD[String]) = { // the interesting stuff happens here
        // it is a separate function so we can test it out
        // the input is an rdd, so we can swap in the testing or big data rdd
        val words = input.flatMap(_.split(" "))
        val kv = words.map(word => (word,1))
        val counts = kv.reduceByKey((x,y) => x+y)
        counts
    }

    def saveit(counts: RDD[(String, Int)], name: String) = {
      counts.saveAsTextFile(name)
    }

}
 
