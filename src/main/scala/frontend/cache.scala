package frontend

import akka.actor.{ReceiveTimeout, ActorRef, Actor, Props
}
import scattergather.{SearchQuery => Query,QueryResponse}
import concurrent.duration._
import akka.actor.ActorLogging

/** A caching actor for the search tree.  This will cache any successful query results into
 * the cache.
 * 
 * Note: this does not *AGE* the cache right now, nor does it go make it up-to-date as
 * the search tree evolves.  In annoying fashion, we leave that as an exercise for the reader :).
 */
class SearchCache(index: ActorRef) extends Actor with debug.DebugActor with ActorLogging {
  def receive: Receive = debugHandler orElse {
    // Hack for behavior...
    case Query("DROP", _) => //Ignore for timeout...
    case q: Query => issueSearch(q)
    case AddToCache(q, r) =>
      log.info("Caching Query ["+q+"] results!")
      cache += q -> r
    case Invalidate(q) =>
      log.info("Invalidating query ["+q+"] cache!")
      cache -= q
  }
  
  def issueSearch(q: Query): Unit = {
    (cache get q.query) match {
        case Some(response) => 
          log.debug("Repsonding ["+q.query+"] with cache!")
          sender ! response
        case _ => 
          // Adapt the query so we have a chance to add it to our cache.
          val client = sender
          val interceptor = context.actorOf(
              Props(new CacheInterceptor(self, client, q.query)))
          index.tell(Query(q.query, q.maxDocs), interceptor)
      }
  }
  // TODO - Pick a data structure that lets us limit the size...
  var cache: Map[String, QueryResponse] = Map.empty
}


/**
 * This Actor is responsible for intercepting query responses and adding them to the cache if
 * they were successful.
 */
class CacheInterceptor(cache: ActorRef, listener: ActorRef, query: String) extends Actor {
  def receive: Receive = {
    case q: QueryResponse =>
      listener ! q
      if(!q.failed) cache ! AddToCache(query, q)
      context stop self
    case ReceiveTimeout =>
      // If the query took to long, w assume a failure, and shut ourselves down.
      // TODO - For nicety, perhaps we should warn the user?
      context stop self
  }
  // TODO - Configure this timeout somewhere globally.
  context setReceiveTimeout 3.seconds
}
// Internal API for the cache...
case class AddToCache(query: String, r: QueryResponse)
case class Invalidate(query: String)
