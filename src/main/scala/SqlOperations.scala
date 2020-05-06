import DataFrameOperations.sparkSession

object SqlOperations extends App with Context{
  // Setup
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  dfTags.createOrReplaceTempView("so_tags")

  sparkSession.catalog.listTables().show()

}
