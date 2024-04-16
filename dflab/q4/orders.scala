
object Q4 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        // do not change this function
        val spark = getSparkSession()
        import spark.implicits._
        val (c, o, i) = getDF(spark)
        val counts = doOrders(c,o,i)
        saveit(counts, "dflabq4")  // save the rdd to your home directory in HDFS
    }


    def doOrders(customers: DataFrame, orders: DataFrame, items: DataFrame): DataFrame = {

    }
    def getTestDF(spark: SparkSession): (DataFrame, DataFrame, DataFrame) = {
        // don't forget the spark.implicits
        // return 3 dfs (customers, orders, items)
    }
    def runTest(spark: SparkSession) = {

    }


}

