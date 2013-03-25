package scattergather

import collection.immutable.HashMap
import akka.actor.{ReceiveTimeout, ActorRef, Actor,Props}
import data.Hotel

case class RestoreHotels(hotels: Seq[Hotel])

trait TopicNode { top: AdaptiveSearchNode =>
  final val maxNoOfDocuments = 10
  // List of Hotel ids we us
  var hotelIds: Seq[String] = Seq.empty
  var documents: Vector[Hotel] = Vector()
  var index: HashMap[String, Seq[(Double, Hotel)]] = HashMap()

  
  def topicNode: PartialFunction[Any, Unit] = {
    // hacks to excercise behavior
    case SearchQuery("BAD", _, r) => r ! QueryResponse(Seq.empty, failed=true)
    case SearchQuery(query, maxDocs, handler) => executeLocalQuery(query, maxDocs, handler)
    case AddHotel(content) => addHotelToLocalIndex(content, updateDb = true)
    case RestoreHotels(hotels) => restoreHotels(hotels)
  }
  
  
  private def executeLocalQuery(query: String, maxDocs: Int, handler: ActorRef) = {
    val result = for {
      results <- (index get query).toSeq
      resultList <- results
    } yield resultList
    handler ! QueryResponse(result take maxDocs)
  }

  private def addHotelToLocalIndex(hotel: Hotel, updateDb: Boolean) = {
    hotelIds = hotelIds :+ hotel.id
    // Save Hotel + Hotel List in a better way, using an actor to ensure it's finished...
    if(updateDb) {
      db ! data.db.DbActor.SaveHotel(hotel)
      db ! data.db.DbActor.SaveTopic(id, hotelIds)
    }
    // TODO - Load the hotel when needed.
    documents = documents :+ hotel

    // TODO - better way to create index...
    if (documents.size > maxNoOfDocuments) split()
    else regenerateIndex()
  }
  
  private def restoreHotels(hotels: Seq[Hotel]): Unit = {
    hotelIds = hotels map (_.id)
    documents = hotels.toVector
    regenerateIndex()
  }
  
  private def regenerateIndex(): Unit = {
    index = HashMap.empty
    for { 
      hotel <- documents
      (key,value) <- hotel.searchContent.split("\\s+").groupBy(identity)
      list = index.get(key) getOrElse Nil
    } index += ((key, ((value.length.toDouble, hotel)) +: list))
  }

  /** Abstract method to split this actor. */
  protected def split(): Unit

  protected def clearIndex(): Unit = {
    documents = Vector()
    index = HashMap()
  }
  
  protected def restoreTopic(): Unit = {
    clearIndex()
    val topic = self
    import akka.actor._
    // TODO - handle failures in this guy...
    context.actorOf(Props(new Actor {
      def receive: Receive = {
        case hotelIds: Seq[String] =>
          db ! data.db.DbActor.GetHotelByIds(hotelIds)
        case hotels: Seq[Hotel] =>
          topic ! RestoreHotels(hotels)
      }
      db ! data.db.DbActor.GetTopicHotelIds(id)
    }), "retore-topic")
  }
}