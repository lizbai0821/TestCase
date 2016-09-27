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
      .appName("test baseline")
      .getOrCreate

    //Parquet location
    val Path: String = "hdfs://dbg11:8020/user/root/test/parquet_bf"

    //read parquet file
    val parquetFileDF = spark.read.parquet(Path)

    //create temp view
    parquetFileDF.createOrReplaceTempView("tbl")

    val option: Int = args(0).toInt

    val result = option match {
      case 1 => spark.sql("select * from tbl where c >= 1433199150 AND " +
        "(yy = 20510950 or zz = 20510950 or xx = 20510950 or p = 20510950 or q = 20510950)" +
        " AND c < 1433199200  AND ccc = '0' limit 5000")
      case 2 => spark.sql("select * from tbl where c >= 1433199150 AND " +
        "(yy = 20510950 or zz = 20510950 or xx = 20510950 or p = 20510950 or q = 20510950)" +
        " AND c < 1433199200  AND (ee <> '' OR (ee = '' AND m = '')) AND ccc = '0' limit 5000")
      case 3 => spark.sql("select * from tbl where c >= 1433199150 AND " +
        "(yy = 20510950 or zz = 20510950 or xx = 20510950 or q = 20510950)" +
        " AND c < 1433199200  limit 5000")
      case 4 => spark.sql("select * from tbl where c >= 1433199150 AND " +
        "(yy = 20510950 or zz = 20510950 or xx = 20510950 or q = 20510950)" +
        " AND c < 1433199200  AND (ee <> '' OR (ee = '' AND m = '')) limit 5000")
      case 5 => spark.sql("select * from tbl where c >= 1433199150 AND" +
        "((yy > 20510000 and yy < 20519999) or (zz > 20510000 and zz <20519999) or (xx > 20510000 and xx < 20519999) or (q > 20510000 and q < 20519999))" +
        "AND c < 1433199200 AND (ee <> '' or (ee = '' and m = '')) limit 5000")
      case 6 => spark.sql("select a,b,c,d,e from tbl where c >= 1433199150 AND" +
        "(yy = 20510950 or zz = 20510950 or xx = 20510950 or p = 20510950 or q = 20510950)" +
        " AND c < 1433199200 limit 5000")
    }
    val resultPath: String = "hdfs://dbg11:8020/user/root/test/bfresult_"
    result.write.parquet(resultPath)
    val logger = LoggerFactory.getLogger(getClass)
    logger.info("suicide")
    spark.sparkContext.stop()

  }
}
