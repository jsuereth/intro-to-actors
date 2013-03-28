import scattergather._
import frontend._
import data._
import data.db.DbActor._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.dispatch.Dispatchers
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
import com.typesafe.config.{ConfigFactory, Config}

object AdaptiveSearchTreeMain {
  
  def loadConfig: Config = ConfigFactory.load()
  
  val system = ActorSystem.create("search-example", loadConfig)
  
  val dbSystem =
    system.actorOf(Props(new data.db.DbSupervisor(data.db.BerkeleyBackend.default)), "search-db")
  
  private def submitInitialDocuments(searchNode: ActorRef) =
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
     dbSystem ! data.db.DbActor.SaveHotel(hotel)
     tree ! AddHotel(hotel) 
    }
  
  def submitTestData(): Unit =
    submitInitialDocuments(tree)
    
  val tree = {
   /* val searchnodedispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("adaptive search tree")
        .withNewThreadPoolWithLinkedBlockingQueueWithCapacity(100)
        .setCorePoolSize(10)
        .setMaxPoolSize(128)
        .setKeepAliveTimeInMillis(60000)
        .setRejectionPolicy(new CallerRunsPolicy)
        .build */
    
    val searchTree = system.actorOf(
        Props(new NodeManager("test", dbSystem))
        .withDispatcher("search-tree-dispatcher"), "search-tree")
    //submitInitialDocuments(searchTree)
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
  
  def query(query: String): Unit =
    cache.tell(_root_.scattergather.SearchQuery(query, 10), echoActor)
  
  def showTree(): Unit = {
    debug.DebugCollector.collectGraph(system, Seq(tree, throttle, frontend, cache))
  }
    
    
  def shutdown(): Unit = system.shutdown()
}


class EchoActor extends Actor {
  def receive: Receive = {
            case f: Function1[_,_] => 
              println(f.getClass)
              println(f.asInstanceOf[Function1[AnyRef, AnyRef]](null))
            case msg => println(msg)
          }
}