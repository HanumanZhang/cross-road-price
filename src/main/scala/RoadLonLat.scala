import java.util

import com.itd.ms.engine.ShootPointEngine
import com.itd.ms.model.{LonLatPoint, RoadModel, SchemeModel}

object CrossingRoadPrice extends Serializable {
  def getLonLatByRoadId(roadId: Int, lon: Double, lat: Double, distance: Double):List[(Int, Double, Double)]={

    val engine: ShootPointEngine = new ShootPointEngine()
    val models: util.List[SchemeModel] = engine.computerShootPoint(roadId,lon,lat,0,distance)
    var reRoadId = 0
    var relon = 0.0
    var relat = 0.0
    for (shm <- 0 until models.size()-1){
      val distance = models.get(shm).getDistance
      val position = models.get(shm).getPosition
      val list: util.List[RoadModel] = models.get(shm).getList
      for(rdm <- 0 until list.size()-1){
        reRoadId = list.get(rdm).getId
        val position2: String = list.get(rdm).getPosition
        val length: Double = list.get(rdm).getLength
        val plist: util.List[LonLatPoint] = list.get(rdm).getPlist
        for(pl <- 0 until plist.size()-1) {
          relon = plist.get(pl).getLon
          relat = plist.get(pl).getLat
        }
      }
    }
    val tuple: (Int, Double, Double) = (reRoadId, relon, relat)
    val tuples: List[(Int, Double, Double)] = List(tuple)
    tuples
  }
}