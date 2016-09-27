/**
  * Created by kaiser on 16/8/5.
  */
/*
modified by Liz on 16/9/14
 */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object DataTest {

  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .config("spark.sql.parquet.enable.bloom.filter","true")
      .config("spark.sql.parquet.bloom.filter.expected.entries","117015,123718,2,61910,65774")
      .config("spark.sql.parquet.bloom.filter.col.name","yy,zz,xx,q,p,w")
      .appName("2.1 rebase: Load parquet with bf")
      .getOrCreate

    //save as parquets
    val path = saveDataAsParquet(spark)

    logger.info("the Loading phase is finished and a test query is issued next")
    //spark.sparkContext.stop()


    //read parquet file
    val parquetFileDF = spark.read.parquet(path)

    parquetFileDF.printSchema()
    logger.info("suicide")
    spark.sparkContext.stop()

  }

  def saveDataAsParquet(spark: SparkSession): String = {
    import spark.implicits._

    //read data
    val dataDF = spark.sparkContext
      .textFile("hdfs://dbg11:8020/user/root/Final/data")
        //.textFile("file:///root/u")
      .map(_.split("\\|"))
      .map(attributes => new VoiceCall(attributes))
      .toDF()

    //save as parquet
    val savePath: String = "hdfs://dbg11:8020/user/root/test/parquet_bf"
    dataDF.write.partitionBy("ccc").parquet(savePath)

    logger.info("SparkSession write task is completed")

    savePath
  }


}
