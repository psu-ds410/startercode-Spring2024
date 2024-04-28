object Q3 {  

    def main(args: Array[String]) = {  // autograder will call this function
        //remember, DataFrames only
    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split(" ").size; case None => 0}})
        spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
    }


    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }

}


