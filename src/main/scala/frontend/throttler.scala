package frontend

import akka.actor.{Actor,ActorRef, ReceiveTimeout, Props}
import concurrent.duration._
import scattergather.QueryResponse

object Defaults {
  val badQueryResponse = QueryResponse(Seq.empty, true)
}

class FrontEndThrottler(tree: ActorRef) extends Actor {
  def receive: Receive = {
    case q: SearchQuery => 
      issueQuery(scattergather.SearchQuery(q.query, q.maxDocs, sender))
    case q: scattergather.SearchQuery =>
      issueQuery(q)
    case TimedOut =>
      Console.println("Query timeout!")
      badQueryCount += 1
    case Success =>
      Console.println("Query Success!")
      if(badQueryCount > 0) badQueryCount -= 1
  }
  
  var badQueryCount: Int = 0
  def issueQuery(q: scattergather.SearchQuery): Unit = 
    if(badQueryCount > 10) {
      Console.println("Fail whale!")
      // Slowly lower count until we're back to normal and allow some through.
      badQueryCount -= 1
      q.gatherer ! Defaults.badQueryResponse
    }
    else {
      val timeout = currentTimeout
      val timer = context.actorOf(Props(new QueryTimer(timeout, q.gatherer, self, 
          Defaults.badQueryResponse)).withDispatcher(context.dispatcher.id))
      tree ! scattergather.SearchQuery(q.query, q.maxDocs, timer)
    }
  def currentTimeout: Duration = 1.seconds
}


case object TimedOut
case object Success

class QueryTimer(timeout: Duration, to: ActorRef, throttler: ActorRef, default: QueryResponse) extends Actor {
  context.setReceiveTimeout(timeout)
  def receive: Receive = {
    case x: scattergather.QueryResponse =>
      throttler ! Success
      to ! x
      context stop self
    case ReceiveTimeout => 
      throttler ! TimedOut
      to ! default
      context stop self
  }
}
