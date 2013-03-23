import scattergather._
import frontend._
import data._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.dispatch.Dispatchers
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
import com.typesafe.config.{ConfigFactory, Config}

object AdaptiveSearchTreeMain {
  
  def loadConfig: Config = ConfigFactory.load()
  
  lazy val system = ActorSystem.create("search-example", loadConfig)
  
  lazy val dbSystem =
    system.actorOf(Props(new data.db.DbSupervisor(data.db.BerkeleyBackend.default)), "search-db")
  
  def submitInitialDocuments(searchNode: ActorRef) =
    Seq(
      Hotel("1", "Hilton", "A nice hotel", Location("123 Street St", "New York", "USA"))
    ) foreach (doc =>  searchNode ! AddHotel(doc))
  lazy val tree = {
   /* val searchnodedispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("adaptive search tree")
        .withNewThreadPoolWithLinkedBlockingQueueWithCapacity(100)
        .setCorePoolSize(10)
        .setMaxPoolSize(128)
        .setKeepAliveTimeInMillis(60000)
        .setRejectionPolicy(new CallerRunsPolicy)
        .build */
    
    val searchTree = system.actorOf(
        Props(new AdaptiveSearchNode(dbSystem))
        .withDispatcher("search-tree-dispatcher"), "search-tree")
    submitInitialDocuments(searchTree)
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
  
  def shutdown(): Unit = system.shutdown()
}