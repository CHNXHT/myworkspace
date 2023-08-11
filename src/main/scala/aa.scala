import org.apache.spark.sql.SparkSession

object aa {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("medical_note")
      .master("local")
      .getOrCreate

    spark.sparkContext.textFile("D:\\工作\\2023\\3月\\省立医院数抽样\\随访_诊断_处方_病历\\sample_note.txt")
      .collect().foreach(_.split(","))
//    val a = spark.read
//      .option("header", true) //是否有header，每个文件的文件头是否存在
//      .option("inferschema", true) //内置的scheme的识别
//      .option("sep", ",") //切割
//      .text("D:\\工作\\2023\\3月\\省立医院数抽样\\随访_诊断_处方_病历\\sample_note.txt")
//    a.printSchema()

  }
}
