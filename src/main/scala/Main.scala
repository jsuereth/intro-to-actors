import scattergather._
import frontend._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.dispatch.Dispatchers
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
import com.typesafe.config.{ConfigFactory, Config}

object AdaptiveSearchTreeMain {
  
  def loadConfig: Config = ConfigFactory.load()
  lazy val system = ActorSystem.create("search-example", loadConfig)
  
  def submitInitialDocuments(searchNode: ActorRef) =
    Seq("Some example data for you",
        "Some more example data for you to use",
        "To be or not to be, that is the question",
        "OMG it's a cat",
        "This is an example.  It's a great one",
        "HAI there", 
        "HAI IZ HUNGRY",
        "Hello, World",
        "Hello, and welcome to the search node 8",
        "The lazy brown fox jumped over the",
        "Winning is the best because it's winning."
    ) foreach (doc =>  searchNode ! SearchableDocument(doc))
  lazy val tree = {
   /* val searchnodedispatcher = Dispatchers.newExecutorBasedEventDrivenDispatcher("adaptive search tree")
        .withNewThreadPoolWithLinkedBlockingQueueWithCapacity(100)
        .setCorePoolSize(10)
        .setMaxPoolSize(128)
        .setKeepAliveTimeInMillis(60000)
        .setRejectionPolicy(new CallerRunsPolicy)
        .build */
    
    val searchTree = system.actorOf(Props(new AdaptiveSearchNode).withDispatcher("search-tree-dispatcher"), "search-tree")
    submitInitialDocuments(searchTree)
    searchTree
  }
  
  lazy val cache = {
    system.actorOf(Props(new SearchCache(tree)).withDispatcher("search-cache-dispatcher"), "search-cache")
  }
  
  lazy val throttle = {
    system.actorOf(Props(new FrontEndThrottler(cache)).withDispatcher("front-end-dispatcher"), "throttler")
  }
  
  lazy val frontend = {
    val frontend = system.actorOf(Props(new FrontEnd(throttle)).withDispatcher("front-end-dispatcher"), "front-end")
    frontend
  }
  
  def shutdown(): Unit = system.shutdown()
}