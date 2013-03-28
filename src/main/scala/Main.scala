import scattergather._
import frontend._
import data._
import data.db.DbActor._
import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.dispatch.Dispatchers
import java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy
import com.typesafe.config.{ConfigFactory, Config}
import akka.actor.ActorLogging
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.UnreachableMember
import akka.cluster.ClusterEvent.ClusterDomainEvent
import akka.cluster.Cluster


object Main {
  
  def main(args: Array[String]): Unit = {
    // Parse args
    if (args.nonEmpty) System.setProperty("akka.remote.netty.tcp.port", args(0))
    else {
      System.err.println(s"Invalid argumnets [${args mkString " "}]!")
      System.err.println("usage: cluster <port> <true|false>")
      System.exit(1)
    }
    val createSearchTree: Boolean = (args drop 1).headOption map (_ == "true") getOrElse false 
    
    // Create an Akka system
    val system = ActorSystem("ClusterSystem")
    
    // Every node starts a front end, but we only start the search tree in one node (for now...)
    if(createSearchTree) runMainClusterNode(system)
    else startFrontEnd(system)
  }

  // NOTE - This is very bad.  IT's a hack because we lamed out and used
  // Berkeley DB instead of Riak.
  def runMainClusterNode(system: ActorSystem): Unit = {
    
    val dbSystem =
      system.actorOf(Props(new data.db.DbSupervisor(data.db.BerkeleyBackend.default)), "search-db")
    saveInitialData(dbSystem)
    
    val frontend = startFrontEnd(system)
    val tree =
      system.actorOf(Props(new NodeManager("top", dbSystem, true))
        .withDispatcher("search-tree-dispatcher"), "search-tree")

    frontend ! RegisterTree(tree)
    
    startWebServer()
  }

  def startFrontEnd(system: ActorSystem): ActorRef =
    system.actorOf(Props[FrontEnd].withDispatcher("search-cache-dispatcher"), "search-front-end")
  
  def startWebServer(): Unit = {
    import unfiltered._
    import request._
    import response._
    import netty._
    object App extends cycle.Plan with cycle.SynchronousExecution with ServerErrorResponse {
      def intent = {
         case GET(Path("/")) => Html(<html>
                                       <head>
                                         <title>WOAH</title>
                                       </head>
                                       <body> HI </body>
                                       </html>)
      }
    }
    //val plan = unfiltered.n
    val http = Http(8080).handler(App).run { s => 
      val d= java.awt.Desktop.getDesktop
      d.browse(new java.net.URI("http://localhost:8080/"))
    }
  }
  
  
  
  def saveInitialData(db: ActorRef) = {
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
    } db ! data.db.DbActor.SaveHotel(hotel)
    // TODO - Save topics and categories.
    db ! data.db.DbActor.SaveTopic("topic-1", Seq("1", "2", "3"))
    db ! data.db.DbActor.SaveTopic("topic-2", Seq("4", "5", "6"))
    db ! data.db.DbActor.SaveTopic("topic-3", Seq("7", "8", "9"))
    db ! data.db.DbActor.SaveTopic("topic-4", Seq("10", "11", "12"))
    db ! data.db.DbActor.SaveCategory("top", Seq("category-6", "category-7"))
    //SearchSystem.dbSystem ! data.db.DbActor.SaveCategory("category-1", Seq("category-6", "category-7"))
    db ! data.db.DbActor.SaveCategory("category-6", Seq("topic-1", "topic-2"))
    db ! data.db.DbActor.SaveCategory("category-7", Seq("topic-3", "topic-4"))
  }
}



object SearchSystem {
  def loadConfig: Config = ConfigFactory.load()
  
  val system = ActorSystem.create("search-example", loadConfig)
  
  val dbSystem =
    system.actorOf(Props(new data.db.DbSupervisor(data.db.BerkeleyBackend.default)), "search-db")
  
  // TODO - Only when not in debug mode...
  saveInitialData()
    
  val tree = {   
    val searchTree = system.actorOf(
        Props(new NodeManager("test", dbSystem, true))
        .withDispatcher("search-tree-dispatcher"), "search-tree")
    searchTree
  }
  
  val frontend = {
    system.actorOf(
        Props(new FrontEnd)
        .withDispatcher("front-end-dispatcher"), "front-end")
  }
  
  frontend ! RegisterTree(tree)
  
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
    SearchSystem.frontend.tell(_root_.scattergather.SearchQuery(query, 10), SearchSystem.echoActor)
  
  def showTree(): Unit = {
    debug.DebugCollector.collectGraph(SearchSystem.system, Seq(SearchSystem.tree, SearchSystem.frontend))
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