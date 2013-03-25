package data
package db

import org.specs2.mutable.Specification
import java.io.File


class BindingsSpec extends Specification {
  val myDbDir = new File("target/test/bindings")
  if(!myDbDir.exists) myDbDir.mkdirs()
  val myDb = new BerkeleyBackend(new File("target/test/bindings"))
  
  def withSession[T](f: PersistentStore => T): T = {
    val session = myDb.open()
    try f(session)
    finally session.close()
  }
  
  "Berkeley databse" should {
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