

object DataFrameOperations extends App with Context {

  // Setup
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/question_tags_10K.csv")
    .toDF("id", "tag")

  //dfTags.show(10)
  //dfTags.select( "tag").show(15)

  //dfTags.select("tag").filter("tag == 'php'").show(10) // count()
  //dfTags.filter("tag like 't%'").show(10) // count()

  /*dfTags.filter("tag like 's%'")
    .filter("id == 25 OR id == 108").count()*/

  print(" ---------------------------- ")

  //dfTags.groupBy("tag").count().filter("count > 5").show(10)
  //dfTags.groupBy("tag").count().filter("count > 5").orderBy("tag").show(10)

  //dfTags.filter("id in (25, 108)").show(10)

  val dfQuestionsCSV = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat","yyyy-MM-dd HH:mm:ss")
    .csv("src/main/resources/questions_10K.csv")
    .toDF("id", "creation_date", "closed_date", "deletion_date", "score", "owner_userid", "answer_count")
  //dfQuestionsCSV.printSchema()

  val dfQuestions = dfQuestionsCSV.select(
    dfQuestionsCSV.col("id").cast("integer"),
    dfQuestionsCSV.col("creation_date").cast("timestamp"),
    dfQuestionsCSV.col("closed_date").cast("timestamp"),
    dfQuestionsCSV.col("deletion_date").cast("date"),
    dfQuestionsCSV.col("score").cast("integer"),
    dfQuestionsCSV.col("owner_userid").cast("integer"),
    dfQuestionsCSV.col("answer_count").cast("integer")
  )

 // dfQuestions.printSchema()
 // dfQuestions.show(10)
   val dfQuestionsSubset = dfQuestions.filter("score > 400 and score < 410").toDF()
   //dfQuestionsSubset.show()
   //dfQuestionsSubset.join(dfTags, "id").show(10)

   /*dfQuestionsSubset.join(dfTags, "id")
     .select("owner_userid", "tag", "creation_date", "score")
     .show(10)*/

     dfQuestionsSubset
     .join(dfTags, dfTags("id") === dfQuestionsSubset("score"))
       .select("owner_userid", "tag", "creation_date")
     .show(10)

}
