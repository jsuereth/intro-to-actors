package scattergather

import akka.actor._
import data.Hotel
import scattergather.NodeManager.BecomeTopic
import scala.collection.immutable.HashMap
import debug.DebugActor


/** This class manages a scatter-gather index node. 
 *  It is responsible for loading, splitting and monitoring the health of its node. 
 */
class NodeManager(val id: String, val db: ActorRef) extends Actor with ActorLogging with DebugActor {
  import NodeManager._
  var current: Option[ActorRef] = None
  val splitter = context.actorOf(Props(new TopicSplitter(db)), "danger-mcgee-" + id)

  // TODO - supervisor strategy for sub-actor...
  
  def sendToCurrent(msg: Any): Unit =
    current foreach (_.tell(msg, sender))
  
  def receive: Receive = debugHandler orElse {
    case q: SearchQuery => 
      // Send down to the child
      // TODO - intercept!
      sendToCurrent(q)
    case add: AddHotel =>
      // TODO Manipulate db for our node?
      sendToCurrent(add)
    // Internal Messages
    case BecomeTopic(hotels) =>
      killAndReplace(context.actorOf(Props(new TopicNode(hotels)), "topic-" + id))
    case BecomeCategory(childIds) =>
      val children =
        for(id <- childIds)
        yield context.actorOf(Props(new NodeManager(id, db)), s"node-manager-$id")
      killAndReplace(context.actorOf(Props(new CategoryNode(children)), "category-"+id))
    case s @ Split(hotels: Seq[Hotel]) =>
      // TODO - figure out how to split...
      log.debug(s"Splitting node[$id]")
      splitter ! TopicSplitter.Split(id, hotels)
      // We continue servicing requests, even as we try to split.
    case Status.Failure(ex) =>
      // Our future has failed, let's issue a better error perhaps?
      log.error(ex, "Failed to load initial state!")
      throw ex
  }
  
  
  // TODO - ping roland about this one.
  override def preStart(): Unit = {
    loadInitialState()
  }
  
  
  // Loads our initial state and send ourselves a "Become" message.
  def loadInitialState(): Unit = {
    import concurrent.Future
    import akka.pattern.pipe
    import context.dispatcher
    import akka.pattern.ask
    import data.db.DbActor._
    import akka.util.Timeout
    import java.util.concurrent.TimeUnit
    implicit val defaultTimeout = Timeout(3, TimeUnit.SECONDS)
    val categoryState: Future[Seq[String]] =
      // We can't use mapTo because it does hard class equivalence, not subclass friendly.
      (db ? GetCategoryTopicNodeIds(id)).asInstanceOf[Future[Seq[String]]]
    val topicState: Future[Seq[data.Hotel]] =
      (db ? GetTopicHotels(id)).mapTo[Seq[data.Hotel]]
     
    val categoryInitMsg = categoryState map { nodeIds =>
      if(nodeIds.isEmpty) sys.error("This is not a category, we cheat and fail the future here")
      log.debug(s"Got Category state [${id}] with nodes [${nodeIds mkString ","}]")
      BecomeCategory(nodeIds)
    }
    val topicInitMsg= topicState map BecomeTopic.apply
    topicState foreach { hotels =>
      log.debug(s"Got TopicState [${id}] [${hotels map (_.id) mkString ", "}]")
    }
    // Feed this future to use when we're ready for it.
    pipe(categoryInitMsg fallbackTo topicInitMsg) to self
  }
  
  def killAndReplace(next: ActorRef): Unit = {
    current foreach (_ ! PoisonPill)
    current = Some(next)
  }
}
object NodeManager {
  sealed trait InternalMessage
  case class BecomeTopic(hotels: Seq[Hotel]) extends InternalMessage
  case class BecomeCategory(children: Seq[String]) extends InternalMessage
  // Construct a new Category after splitting these hotels. 
  case class Split(hotels: Seq[Hotel]) extends InternalMessage
}

/**
 * This class is responsible for splitting a given node into sub-topics,
 * and making that node be a category.
 */
class TopicSplitter(val db: ActorRef) extends Actor with DebugActor {
  import concurrent.Future
  import akka.pattern.{ask, pipe}
  import data.db.DbActor._
  import akka.util.Timeout
  import java.util.concurrent.TimeUnit
  def splitDocSize = 2
  // This actor just attempts to split nodes and then returns
  // to the parent with the result.
  def receive: Receive = debugHandler orElse {
    case TopicSplitter.Split(id, hotels) =>
      val parent = sender
      implicit val timeout = Timeout(3, TimeUnit.SECONDS)
      import context.dispatcher
      // Split the documents and save the topics we're about to load somewhere else.
      val childIdFutures: Seq[Future[String]] =
        for((childId, docs) <- splitIntoTopics(id, hotels))
        yield (db ? SaveTopic(childId, docs map (_.id))) map { ok => childId}
      val becomeCategoryMsg =
        Future sequence childIdFutures map NodeManager.BecomeCategory.apply
      pipe(becomeCategoryMsg) to parent
  }
  /** TODO - do somethign amazing here.
   * Splits the set of hotels we host in the local index into new topics.
   * @return a sequence of topic name -> hosted Hotels.
   */
  def splitIntoTopics(parentId: String, hotels: Seq[Hotel]): Seq[(String, Seq[Hotel])] = 
    for((docs, idx) <- (hotels grouped splitDocSize).zipWithIndex.toSeq)
    yield s"$parentId-$idx" -> docs
}
object TopicSplitter {
  case class Split(id: String, hotels: Seq[Hotel])
}

