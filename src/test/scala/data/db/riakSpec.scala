package data
package db

import org.specs2.mutable.Specification
import java.io.File

class RiakSpec extends Specification {
  val myDb = RiakBackend.default
  
  def withSession[T](f: PersistentStore => T): T = {
    val session = myDb.open()
    try f(session)
    finally session.close()
  }
  
  "Riak databse" should {
    "save and restore hotels" in {
      withSession { session =>
        val hotel = Hotel("Name", "Description", Location("add", "city", "country"))
        session.hotels.put(hotel.id, hotel)
        session.hotels.list.map(_._2) must contain(hotel)
      }
    }
    "save and restore topic nodes" in {
      withSession { session =>
        val node = "1"
        val hotelIds = Seq("2", "3")
        session.topics.put(node, hotelIds)
        session.topics.list must contain(node -> hotelIds)
      }
    }
    "save and restore category nodes" in {
      withSession { session =>
        val node = "4"
        val topicIds = Seq("1", "7")
        session.categories.put(node, topicIds)
        session.categories.list must contain(node -> topicIds)
      }
    }
  }
}