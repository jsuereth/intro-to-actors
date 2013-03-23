package data
package db

import akka.actor.{
  Actor, Props
}

/** A supervisor for the database actor that helps us know when to escalate and
 * when to recover.
 */
class DbSupervisor(backend: StorageBackend) extends Actor {
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._
  import scala.concurrent.duration._
  
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      // TODO - These exceptions should be wrapped.
      case _: com.sleepycat.je.DatabaseNotFoundException |
           _: com.sleepycat.je.EnvironmentLockedException => Escalate 
      case _: com.sleepycat.je.DatabaseException => Restart
      case _: Exception => Restart
    }
  // TODO - Do we need to do this in preStart?
  val child = context.actorOf(Props(new DbActor(backend)), "db-backend")
  
  def receive = {
    case msg: DbActor.DbActorMsg ⇒ child.tell(msg, sender)
  }
}

/**
 * This actor ensures that database access happens in one location,
 * and implements the "unsafe" version of the actor, where failures crash it.
 */
class DbActor(backend: StorageBackend)  extends Actor {
  import DbActor._
  def receive: Receive = {
    case GetHotelByIds(ids) =>
      sender ! ids.flatMap(store.hotels.get)
    case GetCategoryTopicNodeIds(id) =>
      sender ! store.categories.get(id).toSeq.flatten
    case GetTopicHotelIds(topic) =>
      sender ! (store.topics get topic).toSeq.flatten
    case SaveTopic(id, hotels) =>
      store.topics.put(id, hotels)
      sender ! SaveOk
    case SaveCategory(id, nodes) =>
      store.categories.put(id, nodes)
      sender ! SaveOk
    case SaveHotel(hotel) =>
      store.hotels.put(hotel.id, hotel)
      sender ! SaveOk
  }
  
  def store: PersistentStore = {
    if(_store == null) {
      _store = backend.open
    }
    _store
  }
  var _store: PersistentStore = backend.open
  override def preStart(): Unit = {
    if(_store == null)
      _store = backend.open
  }
  // TODO - is preRestart needed?
  override def postStop(): Unit = {
    if(_store != null) store.close()
    _store = null;
  }
}
object DbActor {
  sealed trait DbActorMsg
  // Returns Seq[Hotel]
  case class GetHotelByIds(ids: Seq[String]) extends DbActorMsg
  // Returns Seq[String]
  case class GetCategoryTopicNodeIds(id: String) extends DbActorMsg
  // Returns Seq[String]
  case class GetTopicHotelIds(id: String) extends DbActorMsg
  // Returns SaveOk
  case class SaveTopic(id: String, hotelIdList: Seq[String]) extends DbActorMsg
  // Returns SaveOk
  case class SaveCategory(id: String, nodeIdList: Seq[String]) extends DbActorMsg
  // Doesn't quite fit the "hotels in DBMS" story, but returns SaveOk
  case class SaveHotel(hotel: Hotel) extends DbActorMsg
  // Response to a Save* message
  case object SaveOk
}