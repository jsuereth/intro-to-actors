package scattergather

import collection.immutable.HashMap
import akka.actor.{ReceiveTimeout, ActorRef, Actor,Props}


class AdaptiveSearchNode extends Actor with SearchParent with SearchLeaf {
  // Start as a Leaf Node.
  def receive = leafNode

  /** Splits this search node into a tree of search nodes if there are too many documents. */
  protected def split(): Unit = {
    children = (for((docs, idx) <- documents grouped 5 zipWithIndex) yield {
      // TODO - use search scheduler + hook up to supervisor...
      val child = context.actorOf(
          Props(new AdaptiveSearchNode).withDispatcher("search-tree-dispatcher"), 
          name = "search-node-"+ idx)
      docs foreach (child ! AddHotel(_))
      child
    }).toIndexedSeq
    clearIndex()
    context become parentNode
  }

}