import spark.implicits._

val list = Array(1, 2, 3, 4, 5)

val rdd = sc.parallelize(list)

val df = rdd.toDF("ID")

df.show()