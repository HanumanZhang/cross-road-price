import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}

object CrossRoadPrice {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf()
    conf
      .setMaster("local[*]")
      .setAppName(this.getClass.getName)

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val hiveContext = new HiveContext(sc)

    //使用DW库
    hiveContext.sql("USE DW")
    //从DW库根据给定的道路ID查询到数据,将查询到的数据DataFrame转换为RDD
    /**
      * roadId1 和 roadId2获取
      */
    //通过读取文件获取道路起始结束点，进行广播变量？？
    val linkData: RDD[String] = sc.textFile("hdfs://hadoopslave2:8020/crossingLocation/")

    val roadIdLonLat: RDD[(Int, Double, Double, Int, Double, Double)] = linkData.map(link => {
      //数组里面装的是路口名称、有无红绿灯、linkID+lon+lat
      val roadLonLats: Array[String] = link.split("->")
      //linkId:lon,lat
      //34097606:123.0,60;124.0,61;125.30,62 34097604:123.0,60;124.0,61;125.30,62
      val roadOne: String = roadLonLats(2)
      //linkId:lon,lat linkId:lon,lat
      //[34097606:123.0,60;124.0,61;125.30,62    34097604:123.0,60;124.0,61;125.30,62]
      val linkIdLonLats: Array[String] = roadOne.split(" ")
      //进入的道路的linkId:lon,lat
      //34097606:123.0,60;124.0,61;125.30,62
      val roadStart: String = linkIdLonLats(0)
      //出去的道路的linkId:lon,lat
      //34097604:123.0,60;124.0,61;125.30,62
      val roadEnd: String = linkIdLonLats(linkIdLonLats.length-1)
      //[34097606   123.0,60;124.0,61;125.30,62]
      val linkStartLonLat: Array[String] = roadStart.split(":")
      val linkStartId: Int = linkStartLonLat(0).toInt
      val linkStartLonLats: Array[String] = linkStartLonLat(1).split(";")
      val linkStartLon: Double = linkStartLonLats(linkStartLonLats.length-1).split(",")(0).toDouble
      val linkStartLat: Double = linkStartLonLats(linkStartLonLats.length-1).split(",")(1).toDouble
      val linkEndLonLat: Array[String] = roadEnd.split(":")
      val linkEndId: Int = linkEndLonLat(0).toInt
      val linkEndLonLats: Array[String] = linkStartLonLat(1).split(";")
      val linkEndLon: Double = linkEndLonLats(linkEndLonLats.length-1).split(",")(0).toDouble
      val linkEndLat: Double = linkEndLonLats(linkEndLonLats.length-1).split(",")(1).toDouble
      (linkStartId, linkStartLon, linkStartLat, linkEndId, linkEndLon, linkEndLat)
    })

    //    roadIdLonLat.saveAsTextFile("hdfs://hadoopslave2:8020/roadIdStartEnd")
    /**
      * 将查到的路口数据进行广播
      */
    val tuples: Array[(Int, Double, Double, Int, Double, Double)] = roadIdLonLat.collect()
    val broadcastData: Broadcast[Array[(Int, Double, Double, Int, Double, Double)]] = sc.broadcast(tuples)

    val value: Array[(Int, Double, Double, Int, Double, Double)] = broadcastData.value

    for(va <- value){
      val roadIdOne: Int = va._1
      val roadIdOneLon: Double = va._2
      val roadIdOneLat: Double = va._3
      val roadIdTwo: Int = va._4
      val roadIdTwoLon: Double = va._5
      val roadIdTwoLat: Double = va._6

      val sqlText = "select case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))-1],'$.lon') as Double) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.lon') as Double) end as roadIdTrackOneLon1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))-1],'$.lon') as Double) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.lat') as Double) end as roadIdTrackOneLat1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))-1],'$.DEVtimestamp') as bigint) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.DEVtimestamp') as bigint) end as roadIdTrackOneDEVtimestamp1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))-1],'$.GPStimestamp') as bigint) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.GPStimestamp') as bigint) end as roadIdTrackOneGPStimestamp1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-'))-1],'$.speed') as bigint) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdOne}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.speed') as Double) end as roadIdTrackOneSpeed1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.lat') as Double)as roadIdTrackTwoLon1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.lat') as Double)as roadIdTrackTwoLat1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.DEVtimestamp') as bigint)as roadIdTrackTwoDEVtimestamp1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.GPStimestamp') as bigint)as roadIdTrackTwoGPStimestamp1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
        s"${roadIdTwo}'" +
        "),'$.lonlat'))),'},','}-'),'-')[0],'$.speed') as Double)as roadIdTrackTwoSpeed1 from dw_tbtravel " +
        s"WHERE roadId LIKE '%${roadIdOne}%' AND roadId LIKE '%${roadIdTwo}%'"

      val dataRDD = hiveContext.sql(sqlText)

      val df2Rdd: RDD[Row] = dataRDD.rdd

      val rowRDD: RDD[(Double, Double, Long, Long, Double, Double, Double, Long, Long, Double)] = df2Rdd.map(t => {
        val row = Row(t(0), t(1), t(2), t(3), t(4), t(5), t(6), t(7), t(8), t(9))
        val roadIdTrackOneLon1 = row.getDouble(0)
        val roadIdTrackOneLat1 = row.getDouble(1)
        val roadIdTrackOneDEVtimestamp1 = row.getLong(2)
        val roadIdTrackOneGPStimestamp1 = row.getLong(3)
        val roadIdTrackOneSpeed1 = row.getDouble(4)
        val roadIdTrackTwoLon1 = row.getDouble(5)
        val roadIdTrackTwoLat1 = row.getDouble(6)
        val roadIdTrackTwoDEVtimestamp1 = row.getLong(7)
        val roadIdTrackTwoGPStimestamp1 = row.getLong(8)
        val roadIdTrackTwoSpeed1 = row.getDouble(9)
        (roadIdTrackOneLon1,
          roadIdTrackOneLat1,
          roadIdTrackOneDEVtimestamp1,
          roadIdTrackOneGPStimestamp1,
          roadIdTrackOneSpeed1,
          roadIdTrackTwoLon1,
          roadIdTrackTwoLat1,
          roadIdTrackTwoDEVtimestamp1,
          roadIdTrackTwoGPStimestamp1,
          roadIdTrackTwoSpeed1)
      })

      /**
        * roadIdTrackOneLon1  roadIdTrackOneLat1  roadIdTrackOneDEVtimestamp1  roadIdTrackOneGPStimestamp1 roadIdTrackOneSpeed1
        * roadIdTrackTwoLon1  roadIdTrackTwoLat1  roadIdTrackTwoDEVtimestamp1  roadIdTrackTwoGPStimestamp1  roadIdTrackTwoSpeed1
        */
      val roadCai50: RDD[(Int, Double, Double, Double, Double, Double, Double, Long, Long, Double,
        Int, Double, Double, Double, Double, Double, Double, Long, Long, Double)] = rowRDD.map(ts => {

        /**
          * linkStartId, linkStartLon, linkStartLat, linkEndId, linkEndLon, linkEndLat
          */
        //返回50米的距离地点经纬度
        val linkOne: List[(Int, Double, Double)] = CrossingRoadPrice.getLonLatByRoadId(roadIdOne, roadIdOneLon, roadIdOneLat, -50)
        val linkTwo: List[(Int, Double, Double)] = CrossingRoadPrice.getLonLatByRoadId(roadIdTwo, roadIdTwoLon, roadIdTwoLat, 50)
        val lonOneTo50: Double = linkOne.apply(0)._2
        val latOneTo50: Double = linkOne.apply(0)._3
        val lonTwoTo50: Double = linkTwo.apply(0)._3
        val latTwoTo50: Double = linkTwo.apply(0)._3

        /**
          * linkStartId linkStartLon linkStartLat lonOneTo50 latOneTo50 roadIdTrackOneLon1  roadIdTrackOneLat1  roadIdTrackOneDEVtimestamp1  roadIdTrackOneGPStimestamp1 roadIdTrackOneSpeed1
          * linkEndId linkEndLon linkEndLat lonTwoTo50 latTwoTo50 roadIdTrackTwoLon1  roadIdTrackTwoLat1  roadIdTrackTwoDEVtimestamp1  roadIdTrackTwoGPStimestamp1  roadIdTrackTwoSpeed1
          */
        (roadIdOne, roadIdOneLon, roadIdOneLat, lonOneTo50, latOneTo50, ts._1, ts._2, ts._3, ts._4, ts._5,
          roadIdTwo, roadIdTwoLon, roadIdTwoLat, lonTwoTo50, latTwoTo50, ts._6, ts._7, ts._8, ts._9, ts._10)
      })

      val roadIdTsDisDiff: RDD[(Int, Long, Long, Double, Double, Int, Long, Long, Double, Double)] = roadCai50.map(tt => {
        //计算道路一路口到道路一上的采集点的距离
        val linkOneDistanceToRoad = DistanceUtil.algorithm(tt._2, tt._3, tt._6, tt._7)
        //计算道路二路口到道路二上的采集点的距离
        val linkTwoDistanceToRoad = DistanceUtil.algorithm(tt._12, tt._13, tt._16, tt._17)
        //计算道路一上采集点到道路一上五十米的距离
        val linkOneDistanceTo50 = DistanceUtil.algorithm(tt._4, tt._5, tt._6, tt._7)
        //计算道路二上采集点到道路二上五十米的距离
        val linkTwoDistanceTo50 = DistanceUtil.algorithm(tt._14, tt._15, tt._16, tt._17)
        //计算道路一上采集点到50米的时间
        val linkOneDiffTime: Double = linkOneDistanceTo50 / tt._6
        //计算道路二上采集点到50米的时间
        val linkTwoDiffTime: Double = linkTwoDistanceTo50 / tt._16
        //返回道路ID、采集点时间、距离、时间差
        /**
          *  linkStartId roadIdTrackOneDEVtimestamp1  roadIdTrackOneGPStimestamp1  linkOneDistanceToRoad linkOneDiffTime
          *  linkEndId roadIdTrackTwoDEVtimestamp1  roadIdTrackTwoGPStimestamp1  linkTwoDistanceToRoad linkTwoDiffTime
          *
          */
        (tt._1, tt._8, tt._9, linkOneDistanceToRoad, linkOneDiffTime, tt._11, tt._18, tt._19, linkTwoDistanceToRoad, linkTwoDiffTime)
      })

      val datediff: RDD[RoadPriceBean] = roadIdTsDisDiff.map(rit => {
        var linkOneTime = 0.0
        var linkTwoTime = 0.0
        //道路一50米在前，加上时间差
        if (rit._4 - 50 > 5) {
          linkOneTime = rit._2 + rit._5
          //道路一50米在后，减去时间差
        } else if (rit._4 - 50 < (-5)) {
          linkOneTime = rit._2 - rit._5
          //距离相差在-5到5之间使用当前时间
        } else {
          linkOneTime = rit._2
        }
        //道路二50米在前，加上时间差
        if (rit._9 - 50 > 5) {
          linkTwoTime = rit._7 + rit._10
          //道路二50米在后，减去时间差
        } else if (rit._9 - 50 < (-5)) {
          linkTwoTime = rit._7 - rit._10
          //距离相差在-5到5之间使用当前时间
        } else {
          linkTwoTime = rit._7
        }
        //道路一ID、时间、道路二ID、用时
        /**
          * linkStartId roadIdTrackOneGPStimestamp1 linkEndId 时间差
          */
        val roadIdOne: Int = rit._1
        val dataTime: Int = rit._3.asInstanceOf[Int]
        val roadIdTwo: Int = rit._6
        val timeDiff: Double = linkTwoTime - linkOneTime
        RoadPriceBean(roadIdOne, roadIdTwo, dataTime, timeDiff)
      })

      val df: DataFrame = datediff.toDF()

//      val df: DataFrame = sqlContext.createDataFrame(datediff, RoadPriceBean.getClass)

//      df.registerTempTable("tmp")

      df.registerTempTable("tmp")

//      df.createOrReplaceTempView("tmp")

      val sqlText1 = "select roadIdOne, roadIdTwo as crossroad, hour(from_unixtime(dataTime,'yyyy-MM-dd HH:mm:ss')) as hours, " +
        "avg(timeDiff) as avgTime from tmp group by roadIdOne, roadIdTwo, hour(from_unixtime(dataTime,'yyyy-MM-dd HH:mm:ss'))"

      val frame: DataFrame = sqlContext.sql(sqlText1)

      val rdd = df.rdd

      val historySchema = StructType({
        List(StructField("ROADIDONE", IntegerType),
          StructField("ROADIDTWO", IntegerType),
          StructField("DAYHOUR", IntegerType),
          StructField("TIME", DoubleType)
        )})

      val frame1 = sqlContext.createDataFrame(rdd, historySchema)

      frame1.write
        .format("org.apache.phoenix.spark")
        .mode("overwrite")
        .option("table", "CROSSROADPRICE")
        .option("zkUrl", "192.168.145.79:2181")
        .save()

//      df.write.insertInto("dw_dbAvgTime")

      //      frame.saveToPhoenix("CROSSROADPRICE",new Configuration(),Some("19.168.145.79"))

      //            frame.write
      //              .format("org.apache.phoenix.spark")
      //              .mode("overwrite")
      //              .option("table","CROSSROADPRICE")
      //              .option("zkUrl", "jdbc:phoenix:192.168.59.102:2181")
      //              .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      //              .save()
      //      frame.saveToPhoenix(Map("table" -> "CROSSROADPRICE", "zkUrl" -> "192.168.59.102:2181"))
      //      frame.write
      //        .format("jdbc")
      //        .mode("overwrite")
      //        .option("dbtable","CROSSROADPRICE")
      //        .option("url", "jdbc:phoenix:192.168.59.102:2181")
      //        .option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
      //        .save()
    }
  }
}
case class RoadPriceBean(var roadIdOne: Int, var roadIdTwo: Int, var dataTime: Int, val timeDiff: Double) extends Serializable

case class Lon[Row](
                     roadIdTrackOneLon1: Double,
                     roadIdTrackOneLat1: Double,
                     roadIdTrackOneDEVtimestamp1: Double,
                     roadIdTrackOneGPStimestamp1: Double,
                     roadIdTrackOneSpeed1: Double,
                     roadIdTrackTwoLon1: Double,
                     roadIdTrackTwoLat1: Double,
                     roadIdTrackTwoDEVtimestamp1: Double,
                     roadIdTrackTwoGPStimestamp1: Double,
                     roadIdTrackTwoSpeed1: Double) extends Serializable