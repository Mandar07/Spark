import DataFrameOperations.sparkSession

object SqlOperations extends App with Context{
  // Setup
  val dfTags = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/real_estate.csv")
    .toDF("street", "city",  "zip", "state", "beds", "baths", "sq__ft", "type", "sale_date", "price", "latitude", "longitude")

  dfTags.createOrReplaceTempView("real_estate")

  val dfTagsJ = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/agents_estate.csv")
    .toDF("city", "stores",  "employees")

  dfTagsJ.createOrReplaceTempView("agents_estate")

  //permalink,company,numEmps,category,city,state,fundedDate,raisedAmt,raisedCurrency,round

  val dfTagsK = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/capital_funding.csv")
    .toDF("permalink","company","numEmps","category","city","state","fundedDate","raisedAmt","raisedCurrency","round")

  dfTagsK.createOrReplaceTempView("capital_funding")

  // By accessing DataFrame (row view) from DataSet

  // 1. show the created table structure
  sparkSession.catalog.listTables().show()
  sparkSession.sql("show tables").show()

  //2. select and show some rows
  sparkSession.sql("select * from real_estate limit 10").show()

  //3. Filter by column
  sparkSession.sql("select * from real_estate where city = 'SACRAMENTO'").show(10)

  //4. Group by column , having clause with order BY
  sparkSession.sql("select city,count(*) as count" +
    " from real_estate group by city having count > 10 order by count desc").show(50)

  //5. Join
  sparkSession .sql( // r.*, s.*  for all columns , try with inner, left outer and right outer
    """select r.city, r.price, r.sq__ft, s.stores, s.employees
      |from real_estate r
      |left outer join agents_estate s
      |on r.city = s.city""".stripMargin).show(10)

  //6. Distinct
  sparkSession .sql("""select distinct city from real_estate""".stripMargin) .show(10)

  //7. Average, Min , Max, Sum, Count of columns
  sparkSession.sql("select(avg(price)) from real_estate").show()
  sparkSession.sql("select(max(stores)) from agents_estate").show()
  sparkSession.sql("select(min(employees)) from agents_estate").show()
  sparkSession.sql("select(sum(employees)) from agents_estate").show()
  sparkSession.sql("select(count(beds)) from real_estate where city = 'SACRAMENTO'").show()

  // 7. Where and, or , not
  sparkSession.sql("select city, avg(price), min(sq__ft), max(beds) from real_estate " +
                             "where not city ='SACRAMENTO'" +
                              " having avg(price) > 100000 ").show(10)
  sparkSession.sql("select * from agents_estate " +
                             "where stores > 20 and employees < 300" +
                             " order by city").show()
  // 8. Distinct
  sparkSession.sql("select distinct(company), raisedAmt from capital_funding " +
                            "order by raisedAmt desc").show()

  //9. Offset (ms sql) is not supported in spqrk sql , use zipWithIndex function on rdd for pagination

  //10. In
  sparkSession.sql("select * from real_estate where city" +
                   " in (select city from agents_estate) order by price desc").show(10)

  //11. Like
  sparkSession.sql("select * from real_estate where " +
                    " city like 'S%' order by price desc").show(10)

  sparkSession.sql("select * from real_estate where " +
                     " street like '^[%CT]'").show(10)






}
