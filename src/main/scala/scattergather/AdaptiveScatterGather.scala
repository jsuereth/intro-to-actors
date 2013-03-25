package scattergather

import collection.immutable.HashMap
import akka.actor.{ReceiveTimeout, ActorRef, Actor,Props}
import data.db.DbActor.SaveCategory


class AdaptiveSearchNode(val id: String = "top", val db: ActorRef) extends Actor with CategoryNode with TopicNode {
  // Start as a Topic Node.
  def receive = topicNode

  /** Splits this search node into a tree of search nodes if there are too many documents. */
  protected def split(): Unit = {
    val newChildren = (for((docs, idx) <- documents grouped 5 zipWithIndex) yield {
      // TODO - use search scheduler + hook up to supervisor...
      val childId = id + "-" + idx
      val child = context.actorOf(
          Props(new AdaptiveSearchNode(childId, db)).withDispatcher("search-tree-dispatcher"), 
          name = "search-node-"+ idx)
      docs foreach (child ! AddHotel(_))
      childId -> child
    }).toIndexedSeq
    val childIds = newChildren map (_._1)
    // Make sure we save our split.
    db ! SaveCategory(id, childIds)
    children = newChildren map (_._2)
    clearIndex()
    context become categoryNode
  }
}