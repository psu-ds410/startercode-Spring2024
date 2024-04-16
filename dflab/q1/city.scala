import org.apache.spark.sql.{Dataset, DataFrame, SparkSession, Row}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf

object Q1 {

    def main(args: Array[String]) = {  // this is the entry point to our code
        val spark = getSparkSession()
        import spark.implicits._
        val mydf = getDF(spark) 
        val counts = doCity(mydf) 
        saveit(counts, "dflabq1")  // save the rdd to your home directory in HDFS
    }

    def registerZipCounter(spark: SparkSession) = {
        val zipCounter = udf({x: String => Option(x) match {case Some(y) => y.split(" ").size; case None => 0}})
        spark.udf.register("zipCounter", zipCounter) // registers udf with the spark session
    }

    def doCity(input: DataFrame): DataFrame = {
        input
    }

    def getDF(spark: SparkSession): DataFrame = {
        // when spark reads dataframes from csv, when it encounters errors it just creates lines with nulls in 
        // some of the fields, so you will have to check the slides and the df to see where the nulls are and
        // what to do about them
    }

    def getSparkSession(): SparkSession = {
        val spark = SparkSession.builder().getOrCreate()
        registerZipCounter(spark) // tells the spark session about the UDF
        spark
    }

    def getTestDF(spark: SparkSession): DataFrame = {
        import spark.implicits._ //so you can use .toDF
        // check slides carefully. note that you don't need to add headers, unlike RDDs
    }

    def runTest(spark: SparkSession) = {

    }

    def saveit(counts: DataFrame, name: String) = {
        counts.write.format("csv").mode("overwrite").save(name)

    }

}
