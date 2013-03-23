import data.Hotel

package object scattergather {
    implicit class HotelSearchHelper(val h: Hotel) extends AnyVal {
      def searchContent: String = 
        s"${h.id} ${h.name} ${h.location.city} ${h.location.country} ${h.location.address}"
    }
}