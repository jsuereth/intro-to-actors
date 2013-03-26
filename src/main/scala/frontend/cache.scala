package frontend

import akka.actor.{ReceiveTimeout, ActorRef, Actor, Props
}
import scattergather.{SearchQuery => Query,QueryResponse}
import concurrent.duration._

class SearchCache(index: ActorRef) extends Actor with debug.DebugActor {
  def receive: Receive = debugHandler orElse {
    // Hack for behavior...
    case Query("DROP", _, _) => //Ignore for timeout...
    case q: Query => issueSearch(q)
    case AddToCache(q, r) =>
      println("Caching ["+q+"]!")
      cache += q -> r
    case Invalidate(q) =>
      cache -= q
  }
  
  def issueSearch(q: Query): Unit = {
    (cache get q.query) match {
        case Some(response) => 
          println("Repsonding ["+q.query+"] with cache!")
          q.gatherer ! response
        case _ => 
          // Adapt the query so we have a chance to add it to our cache.
          val int = context.actorOf(
              Props(new CacheInterceptor(self, q.gatherer, q.query))
              .withDispatcher(context.dispatcher.id))
          index ! Query(q.query, q.maxDocs, int)
      }
  }
  // TODO - Pick a data structure that lets us limit the size...
  var cache: Map[String, QueryResponse] = Map.empty
}

class CacheInterceptor(cache: ActorRef, listener: ActorRef, query: String) extends Actor {
  def receive: Receive = {
    case q: QueryResponse =>
      listener ! q
      if(!q.failed) cache ! AddToCache(query, q)
      // TODO - Stop ourselves?
  }
}

case class AddToCache(query: String, r: QueryResponse)
case class Invalidate(query: String)
