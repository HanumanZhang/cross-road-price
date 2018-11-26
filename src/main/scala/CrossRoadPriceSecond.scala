//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{Row, SQLContext}
//import org.apache.spark.sql.hive.HiveContext
//
//object CrossRoadPriceSecond {
//  def main(args: Array[String]): Unit = {
//    System.setProperty("HADOOP_USER_NAME", "root")
//    val conf = new SparkConf()
//    conf
//      .setMaster("local[*]")
//      .setAppName(this.getClass.getName)
//    val sc = new SparkContext(conf)
//    val sqlContext = new SQLContext(sc)
//    import sqlContext.implicits._
//    val hiveContext = new HiveContext(sc)
//
//    //使用DW库
//    hiveContext.sql("USE DW")
//    //从DW库根据给定的道路ID查询到数据,将查询到的数据DataFrame转换为RDD
//    /**
//      * roadId1 和 roadId2获取
//      */
//    //通过读取文件获取道路起始结束点，进行广播变量？？
//    val linkData: RDD[String] = sc.textFile("hdfs://hadoopslave2:8020/crossroadlonlat/")
//
//    val roadIdLonLat: RDD[(Int, Double, Double, Int, Double, Double, Int)] = linkData.map(link => {
//      //数组里面装的是路口名称、有无红绿灯、linkID+lon+lat
//      val roadLonLats: Array[String] = link.split("->")
//      //路口id
//      val intersectionId: Int = roadLonLats(0).toInt
//      //linkId:lon,lat
//      //34097606:123.0,60;124.0,61;125.30,62 34097604:123.0,60;124.0,61;125.30,62
//      val roadOne: String = roadLonLats(2)
//      //linkId:lon,lat linkId:lon,lat
//      //[34097606:123.0,60;124.0,61;125.30,62    34097604:123.0,60;124.0,61;125.30,62]
//      val linkIdLonLats: Array[String] = roadOne.split(" ")
//      //进入的道路的linkId:lon,lat
//      //34097606:123.0,60;124.0,61;125.30,62
//      val roadStart: String = linkIdLonLats(0)
//      //出去的道路的linkId:lon,lat
//      //34097604:123.0,60;124.0,61;125.30,62
//      val roadEnd: String = linkIdLonLats(linkIdLonLats.length-1)
//      //[34097606   123.0,60;124.0,61;125.30,62]
//      val linkStartLonLat: Array[String] = roadStart.split(":")
//      val linkStartId: Int = linkStartLonLat(0).toInt
//      val linkStartLonLats: Array[String] = linkStartLonLat(1).split(";")
//      val linkStartLon: Double = linkStartLonLats(linkStartLonLats.length-1).split(",")(0).toDouble
//      val linkStartLat: Double = linkStartLonLats(linkStartLonLats.length-1).split(",")(1).toDouble
//      val linkEndLonLat: Array[String] = roadEnd.split(":")
//      val linkEndId: Int = linkEndLonLat(0).toInt
//      val linkEndLonLats: Array[String] = linkStartLonLat(1).split(";")
//      val linkEndLon: Double = linkEndLonLats(linkEndLonLats.length-1).split(",")(0).toDouble
//      val linkEndLat: Double = linkEndLonLats(linkEndLonLats.length-1).split(",")(1).toDouble
//
//      /**
//        * 返回
//        * 进入道路：linkId、lon、lat
//        * 出去道路：linkId、lon、lat
//        * 路口ID：intersectionId
//        */
//      (linkStartId, linkStartLon, linkStartLat, linkEndId, linkEndLon, linkEndLat,intersectionId)
//    })
//
//    roadIdLonLat.foreachPartition(t => {
//        val iterator = t.map(tt => {
//        val roadOneId: Int = tt._1
//        val roadIdOneLon: Double = tt._2
//        val roadIdOneLat: Double = tt._3
//        val roadTwoId: Int = tt._4
//        val roadIdTwoLon: Double = tt._5
//        val roadIdTwoLat: Double = tt._6
//        val intersectionId: Int = tt._7
//        val text = s"select ${roadOneId} as inlinkid, ${roadTwoId} as outlinkid, $intersectionId as roadid, $roadIdOneLon as inroadlon, $roadIdOneLat as inroadlat, $roadIdTwoLon as outroadlon, $roadIdTwoLat as outroadlat, case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps," +
//          "'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-'))-1],'$.lon') as Double) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[0],'$.lon') as Double) end as roadIdTrackOneLon1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-'))-1],'$.lon') as Double) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[0],'$.lat') as Double) end as roadIdTrackOneLat1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-'))-1],'$.DEVtimestamp') as bigint) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[0],'$.DEVtimestamp') as bigint) end as roadIdTrackOneDEVtimestamp1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-'))-1],'$.GPStimestamp') as bigint) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[0],'$.GPStimestamp') as bigint) end as roadIdTrackOneGPStimestamp1,case when size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-'))>=2 then cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[size(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-'))-1],'$.speed') as bigint) else cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadOneId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[0],'$.speed') as Double) end as roadIdTrackOneSpeed1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadTwoId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadTwoId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[0],'$.lat') as Double)as roadIdTrackTwoLon1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadTwoId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadTwoId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[0],'$.lat') as Double)as roadIdTrackTwoLat1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadTwoId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadTwoId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[0],'$.DEVtimestamp') as bigint)as roadIdTrackTwoDEVtimestamp1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadTwoId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadTwoId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[0],'$.GPStimestamp') as bigint)as roadIdTrackTwoGPStimestamp1,cast(get_json_object(split(regexp_replace(substring(get_json_object(get_json_object(matgps,'$." +
//          s"${roadTwoId}'" +
//          "),'$.lonlat'),2,length(get_json_object(get_json_object(matgps,'$." +
//          s"${roadTwoId}'" +
//          "),'$.lonlat'))),'},','}-'),'-')[0],'$.speed') as Double)as roadIdTrackTwoSpeed1 from dw_tbcartravel " +
//          s"WHERE roadid LIKE '%${roadOneId}%' AND roadid LIKE '%${roadTwoId}%'"
//        val frame = hiveContext.sql(text)
//        val rdd: RDD[Row] = frame.rdd
//          //进入的linkid、出去的linkid、路口ID、进入最后一个点经度、进入最后一个点纬度、出去第一个点经度、出去第一个点纬度、进入采集经度、进入采集纬度、
//          // dev时间、gps时间、速度、出去的采集经度、采集纬度、dev时间、gps时间、速度
//        rdd
//      })
//      //进入的linkid、出去的linkid、路口ID、进入的最后一个经度、最后一个维度、出去的第一个点的经度、第一个点维度、
//      // 采集经度、采集纬度、采集dev时间、采集gps时间、采集速度、出去的经度、纬度、dev时间、gps时间、速度
//      val list = sc.parallelize(iterator.toList)
//      val tuples: RDD[(Int, Int, Int, Double, Double, Double, Double, Double, Double, Long, Long, Double, Double, Double, Long, Long, Double)] = list.map(tr => {
//        val row: Row = Row(tr(0), tr(1), tr(2), tr(3), tr(4), tr(5), tr(6), tr(7), tr(8), tr(9), tr(10), tr(11),tr(12), tr(13), tr(14), tr(15),tr(16))
//        val roadIdOne: Int = row.getInt(0)
//        val roadIdTwo: Int = row.getInt(1)
//        val intersectionId: Int = row.getInt(2)
//        val inroadLon: Double = row.getDouble(3)
//        val inroadLat: Double = row.getDouble(4)
//        val outroadLon: Double = row.getDouble(5)
//        val outroadLat: Double = row.getDouble(6)
//        val roadIdTrackOneLon1 = row.getDouble(7)
//        val roadIdTrackOneLat1 = row.getDouble(8)
//        val roadIdTrackOneDEVtimestamp1 = row.getLong(9)
//        val roadIdTrackOneGPStimestamp1 = row.getLong(10)
//        val roadIdTrackOneSpeed1 = row.getDouble(11)
//        val roadIdTrackTwoLon1 = row.getDouble(12)
//        val roadIdTrackTwoLat1 = row.getDouble(13)
//        val roadIdTrackTwoDEVtimestamp1 = row.getLong(14)
//        val roadIdTrackTwoGPStimestamp1 = row.getLong(15)
//        val roadIdTrackTwoSpeed1 = row.getDouble(16)
//
//        /**
//          * 进入的linkid、出去的linkid、路口ID、进入的最后一个经度、最后一个维度、出去的第一个点的经度、第一个点维度、
//          * 采集经度、采集纬度、采集dev时间、采集gps时间、采集速度、出去的经度、纬度、dev时间、gps时间、速度
//          */
//        (roadIdOne,
//          roadIdTwo,
//          intersectionId,
//          inroadLon,
//          inroadLat,
//          outroadLon,
//          outroadLat,
//          roadIdTrackOneLon1,
//          roadIdTrackOneLat1,
//          roadIdTrackOneDEVtimestamp1,
//          roadIdTrackOneGPStimestamp1,
//          roadIdTrackOneSpeed1,
//          roadIdTrackTwoLon1,
//          roadIdTrackTwoLat1,
//          roadIdTrackTwoDEVtimestamp1,
//          roadIdTrackTwoGPStimestamp1,
//          roadIdTrackTwoSpeed1)
//      })
//
//      val roadCai50: RDD[(Int, Double, Double, Double, Double, Double, Double, Long, Long, Double, Int, Double, Double, Double, Double, Double, Double, Long, Long, Double, Int)] = tuples.map(iter => {
//        //传入的是进入的linkId、最后一个经纬度，出去的linkId、第一个经纬度
//        val linkOne: List[(Int, Double, Double)] = CrossingRoadPrice.getLonLatByRoadId(iter._1, iter._4, iter._5, -50)
//        val linkTwo: List[(Int, Double, Double)] = CrossingRoadPrice.getLonLatByRoadId(iter._2, iter._6, iter._7, 50)
//        val lonOneTo50: Double = linkOne.apply(0)._2
//        val latOneTo50: Double = linkOne.apply(0)._3
//        val lonTwoTo50: Double = linkTwo.apply(0)._3
//        val latTwoTo50: Double = linkTwo.apply(0)._3
//        /**
//          * 返回：
//          * 进入的
//          * linkId、最后一个点经度、最后一个点纬度、50m经度、50m纬度、采集经度、采集纬度、采集设备时间、采集GPS时间、采集速度
//          * 出去的
//          * linkId、第一个点经度、第一个点纬度、50m经度、50m纬度、采集经度、采集纬度、采集设备时间、采集GPS时间、采集速度、路口ID
//          * linkStartId linkStartLon linkStartLat lonOneTo50 latOneTo50 roadIdTrackOneLon1  roadIdTrackOneLat1  roadIdTrackOneDEVtimestamp1  roadIdTrackOneGPStimestamp1 roadIdTrackOneSpeed1
//          * linkEndId linkEndLon linkEndLat lonTwoTo50 latTwoTo50 roadIdTrackTwoLon1  roadIdTrackTwoLat1  roadIdTrackTwoDEVtimestamp1  roadIdTrackTwoGPStimestamp1  roadIdTrackTwoSpeed1
//          */
//        (iter._1, iter._4, iter._5, lonOneTo50, latOneTo50, iter._8, iter._9, iter._10, iter._11, iter._12,
//          iter._2, iter._6, iter._7, lonTwoTo50, latTwoTo50, iter._13, iter._14, iter._15, iter._16, iter._17, iter._3)
//      })
//      val roadIdTsDisDiff: RDD[(Int, Long, Long, Double, Double, Int, Long, Long, Double, Double, Int)] = roadCai50.map(roadcai50 => {
//        //计算道路一路口到道路一上的采集点的距离(进入道路经度、维度、采集经度纬度)
//        val linkOneDistanceToRoad: Double = DistanceUtil.algorithm(roadcai50._2, roadcai50._3, roadcai50._6, roadcai50._7)
//        //计算道路二路口到道路二上的采集点的距离
//        val linkTwoDistanceToRoad: Double = DistanceUtil.algorithm(roadcai50._12, roadcai50._13, roadcai50._16, roadcai50._17)
//        //计算道路一上采集点到道路一上五十米的距离
//        val linkOneDistanceTo50: Double = DistanceUtil.algorithm(roadcai50._4, roadcai50._5, roadcai50._6, roadcai50._7)
//        //计算道路二上采集点到道路二上五十米的距离
//        val linkTwoDistanceTo50: Double = DistanceUtil.algorithm(roadcai50._14, roadcai50._15, roadcai50._16, roadcai50._17)
//        //计算道路一上采集点到50米的时间
//        val linkOneDiffTime: Double = linkOneDistanceTo50 / ((roadcai50._10 * 1000) / 3600)
//        //计算道路二上采集点到50米的时间
//        val linkTwoDiffTime: Double = linkTwoDistanceTo50 / ((roadcai50._20 * 1000) / 3600)
//        //返回道路ID、采集点时间、距离、时间差
//        /**
//          * 进入：
//          * linkId、设备时间、gps时间、路口到50米距离、时间差
//          * 出去：
//          * linkId、设备时间、gps时间、路口到50米距离、时间差、路口ID
//          * linkStartId roadIdTrackOneDEVtimestamp1  roadIdTrackOneGPStimestamp1  linkOneDistanceToRoad linkOneDiffTime
//          * linkEndId roadIdTrackTwoDEVtimestamp1  roadIdTrackTwoGPStimestamp1  linkTwoDistanceToRoad linkTwoDiffTime
//          */
//        (roadcai50._1, roadcai50._8, roadcai50._9, linkOneDistanceToRoad, linkOneDiffTime, roadcai50._11, roadcai50._18, roadcai50._19, linkTwoDistanceToRoad, linkTwoDiffTime, roadcai50._21)
//      })
//
//      val datediff: RDD[RoadPrice] = roadIdTsDisDiff.map(rit => {
//        var linkOneTime = 0.0
//        var linkTwoTime = 0.0
//        //道路一50米在前，加上时间差
//        if (rit._4 - 50 > 5) {
//          linkOneTime = rit._2 + rit._5
//          //道路一50米在后，减去时间差
//        } else if (rit._4 - 50 < (-5)) {
//          linkOneTime = rit._2 - rit._5
//          //距离相差在-5到5之间使用当前时间
//        } else {
//          linkOneTime = rit._2
//        }
//        //道路二50米在前，加上时间差
//        if (rit._9 - 50 > 5) {
//          linkTwoTime = rit._7 + rit._10
//          //道路二50米在后，减去时间差
//        } else if (rit._9 - 50 < (-5)) {
//          linkTwoTime = rit._7 - rit._10
//          //距离相差在-5到5之间使用当前时间
//        } else {
//          linkTwoTime = rit._7
//        }
//        //道路一ID、时间、道路二ID、用时
//        /**
//          * linkStartId roadIdTrackOneGPStimestamp1 linkEndId 时间差
//          */
//
//        //        val intersectionId: Int = rit._11
//        val intersectionId2: Int = rit._11
//        val roadIdOne: Int = rit._1
//        val dataTime: Int = rit._3.asInstanceOf[Int]
//        val roadIdTwo: Int = rit._6
//        val timeDiff: Double = linkTwoTime - linkOneTime
//
//        /**
//          * 返回对象：
//          * 路口ID、进入道路ID、出去道路ID、时间(2018-12-05 12)、时间差
//          */
//        RoadPrice(intersectionId2, roadIdOne, roadIdTwo, dataTime, timeDiff)
//      })
//
//    })
//  }
//}
//case class RoadPrice(var intersectionId2: Int, var roadIdOne: Int, var roadIdTwo: Int, var dataTime: Int, val timeDiff: Double) extends Serializable
//
//case class Lon[Row](
//  roadIdOne: Int,
//  roadIdTwo: Int,
//  intersectionId: Int,
//  inroadLon: Double,
//  inroadLat: Double,
//  outroadLon: Double,
//  outroadLat: Double,
//  roadIdTrackOneLon1: Double,
//  roadIdTrackOneLat1: Double,
//  roadIdTrackOneDEVtimestamp1: Double,
//  roadIdTrackOneGPStimestamp1: Double,
//  roadIdTrackOneSpeed1: Double,
//  roadIdTrackTwoLon1: Double,
//  roadIdTrackTwoLat1: Double,
//  roadIdTrackTwoDEVtimestamp1: Double,
//  roadIdTrackTwoGPStimestamp1: Double,
//  roadIdTrackTwoSpeed1: Double) extends Serializable