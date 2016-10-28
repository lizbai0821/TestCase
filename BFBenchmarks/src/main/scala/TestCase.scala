/**
  * Created by lizbai on 8/9/16.
  */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object TestCase {
  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession.builder
      .config("spark.sql.parquet.filterPushdown","false")
      .config("spark.sql.parquet.enableVectorizedReader","false")
      .config("spark.sql.parquet.enable.bloom.filter","true")
    //  .config("spark.sql.parquet.bloom.filter.expected.entries","117015,123718,2,61910,65774")
   //   .config("spark.sql.parquet.bloom.filter.col.name","formatcallerno,callerno,orgcalledno,calledno,formatcalledno")
      //.master("local[*]")
      .appName("Base noPushDown"+ "#"+ args(0)) //no bf
     // .appName("updated: Baseline of BF Collect()"+ "#"+ args(0)) //no bf
      .getOrCreate

    //Parquet location
    val Path: String = "hdfs://dbg11:8020/user/root/test/voice_call_parquet_Long" //no bf
    //val Path: String = "hdfs://dbg11:8020/user/root/test/voice_call_parquet_BF_Int" // with bf

    //read parquet file
    val parquetFileDF = spark.read.parquet(Path)

    //parquetFileDF.printSchema()

    //create temp view
    parquetFileDF.createOrReplaceTempView("VOICE_CALL")

    val option: Int = args(0).toInt

    val result = option match {
      case 1 => spark.sql("select * from VOICE_CALL where starttime >= 1433199150 AND " +
        "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or CALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
        " AND starttime < 1433199200  AND last_msisdn = '0' limit 5000")
      case 2 => spark.sql("select * from VOICE_CALL where starttime >= 1433199150 AND " +
        "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or CALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
        " AND starttime < 1433199200  AND (RESERVED3 <> '' OR (RESERVED3 = '' AND RESERVED4 = '')) AND last_msisdn = '0' limit 5000")
      case 3 => spark.sql("select * from VOICE_CALL where starttime >= 1433199150 AND " +
        "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
        " AND starttime < 1433199200 limit 5000")
      case 4 => spark.sql("select * from VOICE_CALL where starttime >= 1433199150 AND " +
        "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
        " AND starttime < 1433199200  AND (RESERVED3 <> '' OR (RESERVED3 = '' AND RESERVED4 = '')) limit 5000")
      case 5 => spark.sql("select * from VOICE_CALL where starttime >= 1433199150 AND" +
        "((FORMATCALLERNO > 20510000 and FORMATCALLERNO < 20519999) or (CALLERNO > 20510000 and CALLERNO <20519999) or (ORGCALLEDNO > 20510000 and ORGCALLEDNO < 20519999) or (FORMATCALLEDNO > 20510000 and FORMATCALLEDNO < 20519999))" +
        "AND starttime < 1433199200 AND (RESERVED3 <> '' or (RESERVED3 = '' and RESERVED4 = '')) limit 5000")
      case 6 => spark.sql("select cs_refid,ngn_refid,starttime,millisec,service_type from VOICE_CALL where starttime >= 1433199150 AND" +
        "(FORMATCALLERNO = 20510950 or CALLERNO = 20510950 or ORGCALLEDNO = 20510950 or CALLEDNO = 20510950 or FORMATCALLEDNO = 20510950)" +
        " AND starttime < 1433199200 limit 5000")
//above are benchmarks

//below are for testing and investigations
      case 7 => spark.sql("select * from VOICE_CALL where starttime >= 1433199150 AND" +
        "((FORMATCALLERNO > 20510000 and FORMATCALLERNO < 20519999) or (CALLERNO > 20510000 and CALLERNO <20519999) or (ORGCALLEDNO > 20510000 and ORGCALLEDNO < 20519999) or (FORMATCALLEDNO > 20510000 and FORMATCALLEDNO < 20519999))" +
        "AND starttime < 1433199200 AND (RESERVED3 <> '' or (RESERVED3 = '' and RESERVED4 = '')) and last_msisdn='0' limit 5000")
      case 8 => spark.sql("select * from VOICE_CALL where starttime >= 1433199150 AND" +
        "((FORMATCALLERNO > 20510000 and FORMATCALLERNO < 20519999) or (CALLERNO > 20510000 and CALLERNO <20519999) or (ORGCALLEDNO > 20510000 and ORGCALLEDNO < 20519999) or (FORMATCALLEDNO > 20510000 and FORMATCALLEDNO < 20519999))" +
        "AND starttime < 1433199200 AND (RESERVED3 <> '' or (RESERVED3 = '' and RESERVED4 = ''))")
    }

      result.collect()
     //val resultPath: String = "hdfs://dbg11:8020/user/root/test/bfresult_"
   // result.write.parquet(resultPath)

    val logger = LoggerFactory.getLogger(getClass)
    logger.info("suicide")
    spark.sparkContext.stop()

  }
}
