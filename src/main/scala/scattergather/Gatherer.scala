package scattergather

import akka.actor.{ReceiveTimeout, ActorRef, Actor}
import concurrent.duration._
import data.Hotel
import debug.DebugActor

/** An actor which receives distributed results and aggregates/responds to the original query. */
case class GathererNode(
    maxDocs: Int,
    query : String,
    maxResponses : Int,
    client : ActorRef) extends Actor with DebugActor {
  
  context.setReceiveTimeout(1.seconds)

  /** Stores the current set of results */
  var results = Seq[(Double, Hotel)]()
  var responseCount = 0
  
  /** Combines the current reuslts with the next set of search results. */
  private def combineResults(current : Seq[(Double, Hotel)], next : Seq[(Double, Hotel)]) =
    (current ++ next).view.sortBy(_._1).take(maxDocs).force

  def receive = debugHandler orElse {
    case QueryResponse(next, false) =>
      results = combineResults(results, next)
      responseCount += 1
      if(responseCount == maxResponses) {
        client ! QueryResponse(results)
        context stop self
      } else context.setReceiveTimeout(1.seconds)
    case QueryResponse(_, true) => // ignore
      context.setReceiveTimeout(1.seconds)
    case ReceiveTimeout  =>
      // TODO - Send a response?
      client ! QueryResponse(Seq(), true)
      context stop self
  }
}
