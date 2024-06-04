import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Author xx
  * Date 2022/7/31
  * Desc 
  */
object SparkAnalysis {


  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName).setMaster("local[*]")
    val spark: SparkSession = SparkSession.builder.config(conf).getOrCreate()


    //读取数据
    val inputDF = spark.read
      .format("csv")
      .option("header", "false")
      .option("sep", "\t")
      .load("output/small_train_format/part-00000")




  }


}
