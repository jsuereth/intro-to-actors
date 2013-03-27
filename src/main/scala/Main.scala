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
  
  lazy val system = ActorSystem.create("search-example", loadConfig)
  
  lazy val dbSystem =
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
     searchNode ! AddHotel(hotel) 
    }
  
  def submitTestData(): Unit =
    submitInitialDocuments(tree)
    
  lazy val tree = {
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
  
  lazy val cache = {
    system.actorOf(
        Props(new SearchCache(tree))
        .withDispatcher("search-cache-dispatcher"), "search-cache")
  }
  
  lazy val throttle = {
    system.actorOf(
        Props(new FrontEndThrottler(cache))
        .withDispatcher("front-end-dispatcher"), "throttler")
  }
  
  lazy val frontend = {
    system.actorOf(
        Props(new FrontEnd(throttle))
        .withDispatcher("front-end-dispatcher"), "front-end")
  }
  
  lazy val echoActor = {
    system.actorOf(
        Props(new Actor {
          def receive: Receive = {
            case msg => println(msg)
          }
        }), "echo-actor")
  }
  
  def query(query: String): Unit = 
    frontend.tell(_root_.frontend.SearchQuery(query, 10), echoActor)
  
  def showTree(): Unit = {
    debug.DebugCollector.collectGraph(system, Seq(tree, throttle, frontend, cache))
  }
    
    
  def shutdown(): Unit = system.shutdown()
}