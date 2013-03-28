package data
package db

import akka.actor.{
  Actor, Props, ActorLogging
}


/** A supervisor for the database actor that helps us know when to escalate and
 * when to recover.
 */
class DbSupervisor(backend: StorageBackend) extends Actor with debug.DebugActor with ActorLogging {
  import akka.actor.OneForOneStrategy
  import akka.actor.SupervisorStrategy._
  import scala.concurrent.duration._
  
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1.minute) {
      // TODO - These exceptions should be wrapped.
      case _: com.sleepycat.je.DatabaseNotFoundException |
           _: com.sleepycat.je.EnvironmentLockedException => Escalate 
      case ex: com.sleepycat.je.DatabaseException => 
        log.debug("Restarting Db actor: ", ex)
        Restart
      case _: Exception => Restart
    }
  // Note: We cannot override preRestart with this here.
  val child = context.actorOf(Props(new DbActor(backend)), "db-backend")
  
  def receive = debugHandler orElse {
    case msg: DbActor.DbActorMsg ⇒ child.tell(msg, sender)
  }
}

/**
 * This actor ensures that database access happens in one location,
 * and implements the "unsafe" version of the actor, where failures crash it.
 */
class DbActor(backend: StorageBackend)  extends Actor with debug.DebugActor with ActorLogging {
  import DbActor._
  def receive: Receive = debugHandler orElse {
    case GetHotelByIds(ids) =>
      sender ! ids.flatMap(store.hotels.get)
    case GetCategoryTopicNodeIds(id) =>
      val nodes = getCategoryNodes(id)
      log.debug(s"Found [$id] nodes: [${nodes mkString ","}]")
      sender ! nodes
    case GetTopicHotels(topic) =>
      val hotels = getTopicHotels(topic)
      log.debug(s"Found [$topic] hotels: [${hotels map (_.id) mkString ","}]")
      sender ! hotels
    case SaveTopic(id, hotels) =>
      log.info(s"Saving topic: [$id] with hotels [${hotels mkString ", "}]")
      store.topics.put(id, hotels)
      sender ! SaveOk
    case SaveCategory(id, nodes) =>
      log.info(s"Saving category: [$id] with nodes [${nodes mkString ", "}]")
      store.categories.put(id, nodes)
      sender ! SaveOk
    case SaveHotel(hotel) =>
      log.debug(s"Saving hotel: [${hotel.id}]")
      store.hotels.put(hotel.id, hotel)
      sender ! SaveOk
  }
  
  private def getCategoryNodes(id: String): Seq[String] =
    store.categories.get(id).toSeq.flatten
  
  private def getTopicHotels(id: String): Seq[Hotel] =
    for {
      hotelIds <- store.topics.get(id).toSeq
      _ = log.debug(s"DbActor: found [$id] hotelIds [${hotelIds mkString ","}]")
      hotelId <- hotelIds
      // TODO - error handling
      hotel <- store.hotels.get(hotelId).toSeq
    } yield hotel
  
  def store: PersistentStore = {
    if(_store == null) {
      _store = backend.open
    }
    _store
  }
  var _store: PersistentStore = null
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
  case class GetTopicHotels(id: String) extends DbActorMsg
  // Returns SaveOk
  case class SaveTopic(id: String, hotelIdList: Seq[String]) extends DbActorMsg
  // Returns SaveOk
  case class SaveCategory(id: String, nodeIdList: Seq[String]) extends DbActorMsg
  // Doesn't quite fit the "hotels in DBMS" story, but returns SaveOk
  case class SaveHotel(hotel: Hotel) extends DbActorMsg
  // Response to a Save* message
  case object SaveOk
}