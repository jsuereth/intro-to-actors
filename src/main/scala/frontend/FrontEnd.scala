package frontend


import akka.actor.{Actor,ActorRef, Props,ActorSystem}
import scattergather.{SearchQuery, QueryResponse}
import akka.actor.ActorLogging
import scala.concurrent.Future

case class RegisterTree(tree: ActorRef)
object FrontEnd {
  def executeQuery(system: ActorSystem, frontend: ActorRef, query: String, maxDocs: Int = 20): Future[Seq[data.Hotel]] = {
    import concurrent._
    import duration._
    import akka.util.Timeout
    import akka.pattern.ask
    import system.dispatcher
    implicit val defaultTimeout = Timeout(3.seconds)
    val result = (frontend ? SearchQuery(query, maxDocs)).mapTo[QueryResponse]
    result map { q =>
      if(q.failed) sys.error("Query failed")
      else q.results.map(_._2)
    }
  }
}
class FrontEnd extends Actor with debug.DebugActor with ActorLogging {
  var searchService: Option[ActorRef] = None
  def receive: Receive = debugHandler orElse {
    case RegisterTree(tree) if searchService.isEmpty =>
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