package scattergather

import collection.immutable.HashMap
import akka.actor.{ReceiveTimeout, ActorRef, Actor,Props}


class AdaptiveSearchNode(db: ActorRef) extends Actor with CategoryNode with TopicNode {
  // Start as a Topic Node.
  def receive = topicNode

  /** Splits this search node into a tree of search nodes if there are too many documents. */
  protected def split(): Unit = {
    children = (for((docs, idx) <- documents grouped 5 zipWithIndex) yield {
      // TODO - use search scheduler + hook up to supervisor...
      val child = context.actorOf(
          Props(new AdaptiveSearchNode(db)).withDispatcher("search-tree-dispatcher"), 
          name = "search-node-"+ idx)
      docs foreach (child ! AddHotel(_))
      child
    }).toIndexedSeq
    clearIndex()
    context become categoryNode
  }

}