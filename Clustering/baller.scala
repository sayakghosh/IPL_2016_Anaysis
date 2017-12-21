import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.sql.functions._

/*
Clustering on:
runsgiven
wickets
economy rate
strike_rate
fourw
*/
val data = sc.textFile("/Users/sayakghosh/Desktop/PES/Sem5/BigData/Project/IPL/balling.csv")
val header = data.first
val rows = data.filter(l => l != header)

case class CC1(country_id:Int, country_name:String,	player_id:Int,	player_name:String,	bowlinnings:Int, balls:Int, runsgiven:Long, wickets:Int, average:Double, economey_rate:Double,  strike_rate:Double,	fourw:Int,	fivew:Int, matches:Int)

val allSplit = rows.map(line => line.split(","))

val allData = allSplit.map(p => CC1(p(0).toInt, p(1).trim.toString, p(2).trim.toInt, p(3).trim.toString, p(4).trim.toInt, p(5).trim.toInt, p(6).trim.toLong, p(7).trim.toInt, p(8).trim.toDouble, p(9).trim.toDouble, p(10).trim.toDouble, p(11).trim.toInt, p(12).trim.toInt, p(13).trim.toInt))

val allDF = allData.toDF()
val rowsRDD = allDF.rdd.map(r => (r.getInt(0), r.getString(1), r.getInt(2), r.getString(3), r.getInt(4), r.getInt(5), r.getLong(6), r.getInt(7), r.getDouble(8), r.getDouble(9), r.getDouble(10), r.getInt(11), r.getInt(12), r.getInt(13) ))

rowsRDD.cache()
val vectors = allDF.rdd.map(r => Vectors.dense( r.getLong(6), r.getInt(7), r.getDouble(9), r.getDouble(10), r.getInt(11)))

vectors.cache()
val kMeansModel = KMeans.train(vectors, 10, 998)

kMeansModel.clusterCenters.foreach(println)

val predictions = rowsRDD.map{r => (r._4, kMeansModel.predict(Vectors.dense( r._7, r._8, r._10, r._11, r._12)))}
val predDF = predictions.toDF("player_name", "CLUSTER")
val t = allDF.join(predDF, "player_name")

t.filter("CLUSTER=0").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sayakghosh/Desktop/clustering-ball/cluster0")
t.filter("CLUSTER=1").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sayakghosh/Desktop/clustering-ball/cluster1")
t.filter("CLUSTER=2").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sayakghosh/Desktop/clustering-ball/cluster2")
t.filter("CLUSTER=3").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sayakghosh/Desktop/clustering-ball/cluster3")
t.filter("CLUSTER=4").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sayakghosh/Desktop/clustering-ball/cluster4")
t.filter("CLUSTER=5").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sayakghosh/Desktop/clustering-ball/cluster5")
t.filter("CLUSTER=6").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sayakghosh/Desktop/clustering-ball/cluster6")
t.filter("CLUSTER=7").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sayakghosh/Desktop/clustering-ball/cluster7")
t.filter("CLUSTER=8").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sayakghosh/Desktop/clustering-ball/cluster8")
t.filter("CLUSTER=9").repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("/Users/sayakghosh/Desktop/clustering-ball/cluster9")
