package frontend


import akka.actor.{Actor,ActorRef}

class FrontEnd(searchService: ActorRef) extends Actor {
  def receive: Receive = {
    case SearchQuery(query, maxDocs) => 
      // TODO - Check response times and issue failure results.
      searchService ! scattergather.SearchQuery(query, maxDocs, sender)
  }
}