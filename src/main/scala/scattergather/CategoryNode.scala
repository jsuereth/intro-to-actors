package scattergather

import collection.immutable.HashMap
import akka.actor.{ReceiveTimeout, ActorRef, Actor,Props}
import debug.DebugActor

case class CategoryChild(props: Props, id: String)

class CategoryNode(childProps: Seq[CategoryChild]) extends Actor with DebugActor {
  val children = childProps map { case CategoryChild(props, id) =>
    context.actorOf(props, id)  
  }
  var currentIdx = 0
  def receive: Receive = debugHandler orElse {
    case SearchQuery(q, max) =>
      // TODO - use gatherer scheduler
      val client = sender
      val gatherer = context.actorOf(Props(new GathererNode(maxDocs = max,
          maxResponses = children.size,
          query = q,
          client = client)))
      for (node <- children) node.tell(SearchQuery(q, max), gatherer)
    case s @ AddHotel(_) => getNextChild ! s
  }
  
  // Round Robin Index Expansion.
  // TODO - Figure out correct *topic/category* for hotel, based on
  // some generated function/matrix
  private def getNextChild = {
    currentIdx = (1 + currentIdx) % children.size
    children(currentIdx)
  }
}