package frontend


import akka.actor.{Actor,ActorRef}

class FrontEnd(searchService: ActorRef) extends Actor with debug.DebugActor {
  def receive: Receive = debugHandler orElse {
    case SearchQuery(query, maxDocs) => 
      // TODO - Check response times and issue failure results.
      searchService.tell(scattergather.SearchQuery(query, maxDocs), sender)
  }
}