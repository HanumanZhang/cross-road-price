import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}

object Demo {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf()
    conf
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val queryDF: DataFrame = sqlContext.load("org.apache.phoenix.spark", Map("table"->"CROSSROADPRICE", "zkUrl"->"192.168.145.79:2181"))

    queryDF.show()

    queryDF.registerTempTable("tmp")
  }
}
