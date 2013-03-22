package scattergather

import akka.actor.{ReceiveTimeout, ActorRef, Actor, Props}
import concurrent.duration.Duration._

class SearchCache(index: ActorRef) extends Actor {
  def receive: Receive = {
    // Hack for behavior...
    case SearchQuery("DROP", _, _) => //Ignore for timeout...
    case q: SearchQuery => issueSearch(q)
    case AddToCache(q, r) =>
      println("Caching ["+q+"]!")
      cache += q -> r
    case Invalidate(q) =>
      cache -= q
  }
  
  def issueSearch(q: SearchQuery): Unit = {
    (cache get q.query) match {
        case Some(response) => 
          println("Repsonding ["+q.query+"] with cache!")
          q.gatherer ! response
        case _ => 
          // Adapt the query so we have a chance to add it to our cache.
          val int = context.actorOf(
              Props(new CacheInterceptor(self, q.gatherer, q.query))
              .withDispatcher(context.dispatcher.id))
          index ! SearchQuery(q.query, q.maxDocs, int)
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
