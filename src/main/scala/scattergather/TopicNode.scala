package scattergather

import collection.immutable.HashMap
import akka.actor.{ReceiveTimeout, ActorRef, Actor,Props}
import data.Hotel

trait TopicNode { top: AdaptiveSearchNode =>
  def maxNoOfDocuments: Int
  // List of Hotel ids we us
  var hotelIds: Seq[String] = Seq.empty
  var documents: Vector[Hotel] = Vector()
  var index: HashMap[String, Seq[(Double, Hotel)]] = HashMap()
  
  
  protected def initTopicNode(hotels: Seq[Hotel]): Unit = {
    hotelIds = hotels map (_.id)
    documents = hotels.toVector
    regenerateIndex()
    context become (topicNode orElse debugHandler)
  }

  
  def topicNode: PartialFunction[Any, Unit] = {
    // hacks to excercise behavior
    case SearchQuery("BAD", _, r) => r ! QueryResponse(Seq.empty, failed=true)
    case SearchQuery(query, maxDocs, handler) => executeLocalQuery(query, maxDocs, handler)
    case AddHotel(content) => addHotelToLocalIndex(content)
  }
  
  
  private def executeLocalQuery(query: String, maxDocs: Int, handler: ActorRef) = {
    val result = for {
      results <- (index get query).toSeq
      resultList <- results
    } yield resultList
    handler ! QueryResponse(result take maxDocs)
  }

  private def addHotelToLocalIndex(hotel: Hotel) = {
    hotelIds = hotelIds :+ hotel.id
    // TODO - Save topic in a more safe way...
    db ! data.db.DbActor.SaveTopic(id, hotelIds)
    // TODO - Load the hotel when needed.
    documents = documents :+ hotel

    // TODO - better way to create index...
    if (documents.size > maxNoOfDocuments) split()
    else regenerateIndex()
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
}