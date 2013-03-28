package frontend

import akka.actor.{Actor,ActorRef, ReceiveTimeout, Props, ActorLogging}
import concurrent.duration._
import scattergather.QueryResponse
import scattergather.SearchQuery

object Defaults {
  val badQueryResponse = QueryResponse(Seq.empty, true)
}

class FrontEndThrottler(tree: ActorRef) extends Actor with debug.DebugActor with ActorLogging {
  def receive: Receive = debugHandler orElse {
    case q: SearchQuery => 
      issueQuery(q, sender)
    case TimedOut =>
      log.debug("Query timeout!")
      badQueryCount += 1
    case Success =>
      log.debug("Query Success!")
      if(badQueryCount > 0) badQueryCount -= 1
  }
  
  var badQueryCount: Int = 0
  def issueQuery(q: scattergather.SearchQuery, client: ActorRef): Unit = 
    if(badQueryCount > 10) {
      log.debug("Fail whale!")
      // Slowly lower count until we're back to normal and allow some through.
      badQueryCount -= 1
      client ! Defaults.badQueryResponse
    } else {
      val timeout = currentTimeout
      val me = self
      val timer = context.actorOf(Props(new QueryTimer(timeout, client, me, 
          Defaults.badQueryResponse)).withDispatcher(context.dispatcher.id))
      tree.tell(scattergather.SearchQuery(q.query, q.maxDocs), timer)
    }

  def currentTimeout: Duration = 1.seconds
}


case object TimedOut
case object Success

class QueryTimer(timeout: Duration, to: ActorRef, throttler: ActorRef, defaultResponse: QueryResponse) extends Actor {
  context.setReceiveTimeout(timeout)
  def receive: Receive = {
    case x: scattergather.QueryResponse =>
      throttler ! Success
      to ! x
      context stop self
    case ReceiveTimeout => 
      throttler ! TimedOut
      to ! defaultResponse
      context stop self
  }
}
