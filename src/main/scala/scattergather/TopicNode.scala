package scattergather

import collection.immutable.HashMap
import akka.actor.{ReceiveTimeout, ActorRef, Actor,Props}
import data.Hotel
import debug.DebugActor

class TopicNode(hotels: Seq[Hotel]) extends Actor with DebugActor {
  def maxNoOfDocuments: Int = 10
  // List of Hotel ids we us
  var hotelIds: Seq[String] = hotels map (_.id)
  var documents: Vector[Hotel] = hotels.toVector
  var index: HashMap[String, Seq[(Double, Hotel)]] = HashMap()
  
  def receive: Receive = debugHandler orElse {
    // Hacks so we can test bad queries.
    case SearchQuery("BAD", _) => sender ! QueryResponse(Seq.empty, failed=true)
    case SearchQuery(query, maxDocs) => executeLocalQuery(query, maxDocs, sender)
    case AddHotel(content) => addHotelToLocalIndex(content)
  }
  
  override def preStart(): Unit = regenerateIndex()

  /** Tell our parent to split as as soon as it is able... */
  private def split(): Unit =
    context.parent ! NodeManager.Split(hotels)
  
  private def addHotelToLocalIndex(hotel: Hotel) = {
    hotelIds = hotelIds :+ hotel.id
    // TODO - Load the hotel when needed.
    documents = documents :+ hotel
    // Tell our parent we need to be saved with the new documents...
    context.parent ! NodeManager.SaveTopic(documents)
    // Even if we split, we continue serving documents until the new
    // node is ready.
    if (documents.size > maxNoOfDocuments) split()
    regenerateIndex()
  }
  
  private def executeLocalQuery(query: String, maxDocs: Int, handler: ActorRef) = {
    val result = for {
      results <- (index get query).toSeq
      resultList <- results
    } yield resultList
    handler ! QueryResponse(result take maxDocs)
  }
  
  private def regenerateIndex(): Unit = {
    index = HashMap.empty
    for { 
      hotel <- documents
      (key,value) <- hotel.searchContent.split("\\s+").groupBy(identity)
      list = index.get(key) getOrElse Nil
    } index += ((key, ((value.length.toDouble, hotel)) +: list))
  }
  
}