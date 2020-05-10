import java.io.File
import org.apache.spark.sql.DataFrame

object DataFrameOperations extends App with Context {

  val file =  new File("src/main/resources/capital_funding.parquet")
  var parquetFileDF : DataFrame = null
    if (file.exists() ) {
      parquetFileDF= sparkSession.read.parquet("src/main/resources/capital_funding.parquet")
  }
    else {
      val csvFile = sparkSession.read.csv("src/main/resources/capital_funding.csv")
      csvFile.write.parquet("src/main/resources/capital_funding.parquet")
      parquetFileDF = sparkSession.read.parquet("src/main/resources/capital_funding.parquet")
    }

  val dfintTags = parquetFileDF.toDF("permalink","company","numEmps","category","city","state","fundedDate",
    "raisedAmt","raisedCurrency","round")

  val dfTags = dfintTags.select(
    dfintTags.col("permalink").cast("string"),
    dfintTags.col("company").cast("string"),
    dfintTags.col("numEmps").cast("integer"),
    dfintTags.col("category").cast("string"),
    dfintTags.col("city").cast("string"),
    dfintTags.col("state").cast("string"),
    dfintTags.col("fundedDate").cast("timestamp"),
    dfintTags.col("raisedAmt").cast("long"),
    dfintTags.col("raisedCurrency").cast("string"),
    dfintTags.col("round").cast("string")
  )

  // 1. print schema
  dfTags.printSchema()

  //2. select
  dfTags.select("*").show(10)
  dfTags.select("company", "numEmps", "raisedAmt").show(10)

  //3. group by filter
  dfTags.groupBy("city").count().filter("company = 'Facebook'").show()

  //4. multi filter chaining, sort
  dfTags.filter("city = 'San Francisco'")
    .filter("round = 'c' or round = 'b'")
    .sort(dfTags("raisedAmt").desc).show(10)

  //5.like with cast
  dfTags.filter("round like 'c%'").show(10)

  // Text file rdd operations

  val file =  new File("src/main/resources/sample.parquet")
  var parquetFileTxtDF : DataFrame = null
  if (file.exists() ) {
    parquetFileTxtDF = sparkSession.read.parquet("src/main/resources/sample.parquet")
  }
  else {
    val csvFile = sparkSession.read.textFile("src/main/resources/sample.txt")
    csvFile.write.parquet("src/main/resources/sample.parquet")
    parquetFileTxtDF = sparkSession.read.parquet("src/main/resources/sample.parquet")
  }

  val dfTagsT = parquetFileTxtDF.toDF()

  //1. count lines
  val countLines = dfTagsT.count()
  println(s"$countLines")




}
