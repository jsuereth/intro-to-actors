package scattergather

import collection.immutable.HashMap
import akka.actor.{ReceiveTimeout, ActorRef, Actor,Props}


trait CategoryNode { self: AdaptiveSearchNode =>
 var children = IndexedSeq[ActorRef]()
 var currentIdx = 0
 
 /** Someone else knows how to create search children. */
 def makeChild(childId: String): ActorRef
 
 protected def initCategoryNode(childrenIds: Seq[String]): Unit = {
   children = (childrenIds map makeChild).toIndexedSeq
   context become (categoryNode orElse debugHandler)
 }
 
 def categoryNode: PartialFunction[Any, Unit] = {
    case SearchQuery(q, max, responder) =>
        // TODO - use gatherer scheduler
        val gatherer = context.actorOf(Props(new GathererNode(maxDocs = max,
            maxResponses = children.size,
            query = q,
          client = responder)).withDispatcher(context.dispatcher.id))
        for (node <- children) {
          node ! SearchQuery(q, max, gatherer)
        }
    case s @ AddHotel(_) => getNextChild ! s
  }

  // Round Robin
  private def getNextChild = {
    currentIdx = (1 + currentIdx) % children.size
    children(currentIdx)
  }

}