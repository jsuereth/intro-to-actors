package scattergather

import akka.actor._
import data.Hotel
import scattergather.NodeManager.BecomeTopic
import scala.collection.immutable.HashMap
import debug.DebugActor
/**
 * This class is responsible for splitting a given node into sub-topics,
 * and making that node be a category.
 */
class TopicSplitter(val db: ActorRef) extends Actor {
  import concurrent.Future
  import akka.pattern.{ask, pipe}
  import data.db.DbActor._
  import akka.util.Timeout
  import java.util.concurrent.TimeUnit
  def splitDocSize = 2
  // This actor just attempts to split nodes and then returns
  // to the parent with the result.
  def receive: Receive = {
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

