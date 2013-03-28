import scattergather._
import frontend._
import data._
import data.db.DbActor._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.dispatch.Dispatchers
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
import com.typesafe.config.{ConfigFactory, Config}


object SearchSystem {
  def loadConfig: Config = ConfigFactory.load()
  
  val system = ActorSystem.create("search-example", loadConfig)
  
  val dbSystem =
    system.actorOf(Props(new data.db.DbSupervisor(data.db.BerkeleyBackend.default)), "search-db")
  
  // TODO - Only when not in debug mode...
  saveInitialData()
    
  val tree = {   
    val searchTree = system.actorOf(
        Props(new NodeManager("test", dbSystem))
        .withDispatcher("search-tree-dispatcher"), "search-tree")
    searchTree
  }
  
  val cache = {
    system.actorOf(
        Props(new SearchCache(tree))
        .withDispatcher("search-cache-dispatcher"), "search-cache")
  }
  
  val throttle = {
    system.actorOf(
        Props(new FrontEndThrottler(cache))
        .withDispatcher("front-end-dispatcher"), "throttler")
  }
  
  val frontend = {
    system.actorOf(
        Props(new FrontEnd(throttle))
        .withDispatcher("front-end-dispatcher"), "front-end")
  }
  
  val echoActor =
    system.actorOf(Props[EchoActor], "echo-actor")
    
  def shutdown(): Unit = system.shutdown()
  
  
  def saveInitialData() =
    for { hotel <- Seq(
        Hotel("1", "Hilton 1", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("2", "Hilton 2", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("3", "Hilton 3", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("4", "Hilton 4", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("5", "Hilton 5", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("6", "Hilton 6", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("7", "Hilton 7", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("8", "Hilton 8", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("9", "Hilton 9", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("10", "Hilton 10", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("11", "Hilton 11", "A nice hotel", Location("123 Street St", "New York", "USA")),
        Hotel("12", "Hilton 12", "A nice hotel", Location("123 Street St", "New York", "USA"))
      )
    } {
     SearchSystem.dbSystem ! data.db.DbActor.SaveHotel(hotel)
     // TODO - Save topics and categories.
     SearchSystem.dbSystem ! data.db.DbActor.SaveTopic("topic-1", Seq("1", "2", "3"))
     SearchSystem.dbSystem ! data.db.DbActor.SaveTopic("topic-2", Seq("4", "5", "6"))
     SearchSystem.dbSystem ! data.db.DbActor.SaveTopic("topic-3", Seq("7", "8", "9"))
     SearchSystem.dbSystem ! data.db.DbActor.SaveTopic("topic-4", Seq("10", "11", "12"))
     SearchSystem.dbSystem ! data.db.DbActor.SaveCategory("test", Seq("category-6", "category-7"))
     //SearchSystem.dbSystem ! data.db.DbActor.SaveCategory("category-1", Seq("category-6", "category-7"))
     SearchSystem.dbSystem ! data.db.DbActor.SaveCategory("category-6", Seq("topic-1", "topic-2"))
     SearchSystem.dbSystem ! data.db.DbActor.SaveCategory("category-7", Seq("topic-3", "topic-4"))
    }
}

object AdaptiveSearchTreeMain {
  
  def query(query: String): Unit =
    SearchSystem.cache.tell(_root_.scattergather.SearchQuery(query, 10), SearchSystem.echoActor)
  
  def showTree(): Unit = {
    debug.DebugCollector.collectGraph(SearchSystem.system, Seq(SearchSystem.tree, SearchSystem.throttle, SearchSystem.frontend, SearchSystem.cache))
  }
    
    
  def shutdown(): Unit = SearchSystem.shutdown()
}


class EchoActor extends Actor {
  def receive: Receive = {
            case f: Function1[_,_] => 
              println(f.getClass)
              println(f.asInstanceOf[Function1[AnyRef, AnyRef]](null))
            case msg => println(msg)
          }
}