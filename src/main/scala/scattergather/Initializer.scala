package scattergather

import akka.actor.ActorRef
import akka.actor.ReceiveTimeout
import akka.util.Timeout
import java.util.concurrent.TimeUnit
import akka.pattern.{ask, pipe}
import scala.concurrent.Future
import data.db.DbActor.{GetTopicHotels, GetCategoryTopicNodeIds}

sealed trait InitNodeMsg
case class InitCategory(nodeIds: Seq[String]) extends InitNodeMsg
case class InitTopic(hotels: Seq[data.Hotel]) extends InitNodeMsg

// This trait keeps track of how to intialize the adaptive search node...
trait Initializer { self: AdaptiveSearchNode =>
  def db: ActorRef  
  def id: String

  // We should initialize in 3 seconds or less...
  implicit def defaultTimeout = Timeout(3, TimeUnit.SECONDS)
  
  final def getCategoryState: Future[Seq[String]] =
    // We can't use mapTo because it does hard class equivalence, not subclass friendly.
    (db ? GetCategoryTopicNodeIds(id)).asInstanceOf[Future[Seq[String]]]
  final def getTopicState: Future[Seq[data.Hotel]] =
    (db ? GetTopicHotels(id)).mapTo[Seq[data.Hotel]]
     
  final def getInitialState: Future[InitNodeMsg] = {
    import context.dispatcher
    // TODO - import executor from current dispatcher
    val categoryState = getCategoryState map { nodeIds =>
      if(nodeIds.isEmpty) sys.error("This is not a category, we cheat and fail the future here")
      log.debug(s"Got Category state [${id}] with nodes [${nodeIds mkString ","}]")
      InitCategory(nodeIds)
    }
    val topicState = getTopicState map InitTopic.apply
    topicState foreach { topics =>
      log.debug(s"Got TopicState [${id}] [${topics.hotels map (_.id) mkString ", "}]")
    }
    categoryState fallbackTo topicState
  }
  
  final def loadInitialState(): Unit = {
    import context.dispatcher
    pipe(getInitialState) to context.self
    context setReceiveTimeout defaultTimeout.duration
  }
  
  
  def waitForInitialState: Receive = {
    case InitTopic(hotels) => 
      log.debug(s"Initializing topic with hotels [${hotels map (_.id) mkString ","}]")
      initTopicNode(hotels)
    case InitCategory(nodes) => 
      log.debug(s"Initializing category with nodes [${nodes mkString ","}]")
      initCategoryNode(nodes)
    // We're the top level, or there's an error.... 
    // Maybe we need to detect if we should bomb here...
    case ReceiveTimeout => 
      log.error(s"failed to load configuration for $id")
      initTopicNode(Seq.empty)
  }
}