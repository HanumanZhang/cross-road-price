object DistanceUtil {
  /**
    * 通过经纬度计算两点之间的距离
    * @param longitudeOne 第一个点的经度
    * @param latitudeOne 第一个点的纬度
    * @param longitudeTwo 第二个点的经度
    * @param latitudeTwo 第二个点的纬度
    * @return 返回计算后两点之间的距离
    */
  def algorithm(longitudeOne: Double, latitudeOne: Double, longitudeTwo: Double, latitudeTwo: Double): Double = {
    //计算两点的经度差、纬度差
    val lon: Double = rad(longitudeOne)-rad(longitudeTwo)
    val lat: Double = rad(latitudeOne)-rad(latitudeTwo)
    //计算两点的弧长距离
    var distance: Double = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(lat/2),2) + Math.cos(latitudeOne) * Math.cos(latitudeTwo) * Math.pow(Math.sin(lon/2),2)))
    //两点的弧长乘上地球半径，半径单位为米
    distance = distance * 6378137.0
    //返回精确距离的数值
    Math.round(distance * 10000d)/10000d
  }
  /**
    * 将经纬度的角度转换为弧度
    * @param args 经度或者纬度
    * @return 返回转换后的经纬度对应的弧度
    */
  def rad(args: Double): Double = {
    args * Math.PI / 180.00
  }
}
