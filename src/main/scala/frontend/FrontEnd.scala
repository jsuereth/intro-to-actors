package frontend


import akka.actor.{Actor,ActorRef, Props}
import scattergather.SearchQuery
import akka.actor.ActorLogging

case class RegisterTree(tree: ActorRef)

class FrontEnd extends Actor with debug.DebugActor with ActorLogging {
  var searchService: Option[ActorRef] = None
  def receive: Receive = debugHandler orElse {
    case RegisterTree(tree) =>
      log.info(s"Registering search tree: [$tree] with current frontend [$self]")
      // Now we create our underlying tree...
      val cache = context.actorOf(
          Props(new SearchCache(tree)).withDispatcher("search-cache-dispatcher"), 
          "search-cache")
      val throttler = context.actorOf(
          Props(new FrontEndThrottler(cache)), 
          "search-throttler")
      searchService = Some(throttler)
    case SearchQuery(query, maxDocs) => 
      // TODO - Issue failure if we don't have a search service yet...
      // or save messages....
      searchService foreach {
        _.tell(scattergather.SearchQuery(query, maxDocs), sender)
      }
  }
}