
object RddOperations extends App with Context{

  val rddTxt = sparkSession.sparkContext.textFile("src/main/resources/sample.txt")

  //1. count words & chars
  val countWords = rddTxt.map(_.split("").length).collect()
  val countChars = rddTxt.map(_.length).collect()
  val mapPair = rddTxt.flatMap(_.split("\\s")).map(x=> (x,1)).reduceByKey((x, y)=>x+y).collect()
  val mapSortedPair = rddTxt.flatMap(_.split("\\s")).distinct.map(x=>(x,1)).sortByKey().collect()
  val mapAggPair = rddTxt.flatMap(_.split("\\s")).distinct.map(x=>(x,1)).aggregateByKey()
  val mapCountPair = rddTxt.flatMap(_.split("\\s")).distinct.map(x=>(x,1)).countByKey()


  // 2. filter
  val arrStr = rddTxt.filter(_.contains("LETTER")).collect()

  val studentRDD = sparkSession.sparkContext.parallelize(Array(
    ("Joseph", "Maths", 83), ("Joseph", "Physics", 74), ("Joseph", "Chemistry", 91), ("Joseph", "Biology", 82),
    ("Jimmy", "Maths", 69), ("Jimmy", "Physics", 62), ("Jimmy", "Chemistry", 97), ("Jimmy", "Biology", 80),
    ("Tina", "Maths", 78), ("Tina", "Physics", 73), ("Tina", "Chemistry", 68), ("Tina", "Biology", 87),
    ("Thomas", "Maths", 87), ("Thomas", "Physics", 93), ("Thomas", "Chemistry", 91), ("Thomas", "Biology", 74),
    ("Cory", "Maths", 56), ("Cory", "Physics", 65), ("Cory", "Chemistry", 71), ("Cory", "Biology", 68),
    ("Jackeline", "Maths", 86), ("Jackeline", "Physics", 62), ("Jackeline", "Chemistry", 75), ("Jackeline", "Biology", 83),
    ("Juan", "Maths", 63), ("Juan", "Physics", 69), ("Juan", "Chemistry", 64), ("Juan", "Biology", 60)), 3)

  //1. find max marks
  def seqOp1 = (accum: Int, element: (String, Int)) =>
    if(accum > element._2) accum else element._2

  def combOp1 = (accum1: Int, accum2: Int) =>
    if(accum1 > accum2) accum1 else accum2

  val zeroVal1 = 0
  val aggrRDD1 = studentRDD.map(t => (t._1, (t._2, t._3)))
    .aggregateByKey(zeroVal1)(seqOp1, combOp1)

  aggrRDD1.collect foreach println

  //2. print subject name alongside

  def seqOp2 = (accum: (String, Int), element: (String, Int)) =>
    if(accum._2 > element._2) accum else element

  def combOp2 = (accum1: (String, Int), accum2: (String, Int)) =>
    if(accum1._2 > accum2._2) accum1 else accum2

  val zeroVal2 = ("", 0)

  val aggrRDD2 = studentRDD.map(t => (t._1, (t._2, t._3)))
    .aggregateByKey(zeroVal2)(seqOp2, combOp2)

  aggrRDD2.collect foreach println


}
