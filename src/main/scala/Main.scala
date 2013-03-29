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


import unfiltered.response._
import akka.actor.PoisonPill


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
    if(createSearchTree) startMainNode(system)
    else startSecondaryNode(system)
  }

  // NOTE - This is very bad.  IT's a hack because we lamed out and used
  // Berkeley DB instead of Riak.
  def startMainNode(system: ActorSystem): Unit = {
    
    val dbSystem =
      system.actorOf(Props(new data.db.DbSupervisor(data.db.BerkeleyBackend.default)), "search-db")
    saveInitialData(dbSystem)
    
    val frontend = startFrontEnd(system)
    
    
    
    val tree = //startTreeSingleton(system, dbSystem)
    
      system.actorOf(Props(new TreeTop("top", dbSystem))
        .withDispatcher("search-tree-dispatcher"), "search-back-end")
    startWebServer(system, frontend, 8888, Some(tree))
  }

  def startTreeSingleton(system: ActorSystem, db: ActorRef): ActorRef = {
    import akka.contrib.pattern.ClusterSingletonManager
    system.actorOf(Props(
        new ClusterSingletonManager(
          singletonProps = _ => Props(new TreeTop("top", db)),
          singletonName = "consumer",
          terminationMessage = PoisonPill,
          role = None)),
       name = "singleton")
  }
  
  def startSecondaryNode(system: ActorSystem) {
    val frontend = startFrontEnd(system)
    startWebServer(system, frontend, 8889)
  }
  
  def startFrontEnd(system: ActorSystem): ActorRef =
    system.actorOf(Props[FrontEnd].withDispatcher("search-cache-dispatcher"), "search-front-end")
  
  def startWebServer(system: ActorSystem, frontend: ActorRef, port: Int, backend: Option[ActorRef] = None): Unit = {
    import unfiltered._
    import request._
    import response._
    import netty._
    import scala.concurrent.Await
    import scala.concurrent.duration._
    object App extends cycle.Plan with cycle.SynchronousExecution with ServerErrorResponse {
      
      object QueryParam extends Params.Extract("q", Params.first)
      
      def resource(name: String): java.io.File =
        new java.io.File("www" + name)
      
      def intent = {
        case GET(Path("/")) => 
          mainView("Hotel Search App", 
              css = Seq("/index.css"),
              js = Seq("/jquery.js", "/index.js")) {
             <h1> Search for a Hotel!</h1>
             <input type="text" id="searchBox"></input><button id="searchButton">Search</button>
             <div id="hotelList"></div>
             <p class="footer">You're on <a href="/admin">{nodeName}</a></p>
          }
        case (Path("/query") & Params(QueryParam(query))) => 
            try {
              val result = Await.result(_root_.frontend.FrontEnd.executeQuery(system, frontend, query), 3.seconds)
              def toJson(hotel: data.Hotel): String =
                s"""|{
                    |  "id": "${hotel.id}",
                    |  "name": "${hotel.name}",
                    |  "description": "${hotel.description}",
                    |  "city": "${hotel.location.city}"
                    |}""".stripMargin
            // Run the query and respond!
            JsonContent ~> ResponseString(s"""|{
                               |  "success":"true",
                               |  "results": [
                               |    ${result map toJson mkString ","}
                               |  ]               
                               |}""".stripMargin)
            } catch {
              case ex: Exception =>
                JsonContent ~> ResponseString(s"""{ "success": "false" }""")
            }
          
        case GET(Path("/admin")) => 
          mainView("Hotel Search Admin") { 
            <h1> Node {nodeName} </h1>
            <img src="/tree.png"/>
          }
        case GET(Path("/tree.png")) =>
          FileResponse(Await.result(debug.DebugCollector.collectGraph(system, backend.toSeq :+ frontend), 5.seconds))
        case GET(Path(name)) if resource(name).isFile =>
          FileResponse(resource(name))
      }
      
      def nodeName: String = s"Node [${sys.props("akka.remote.netty.tcp.port")}]"

      def mainView(title: String,
          css: Seq[String] = Seq.empty,
          js: Seq[String] = Seq.empty
          )(body: scala.xml.NodeSeq): response.Html =
        Html(<html>
               <head>
                 <title>HOTEL SEARCHY APP</title>
                 {
                   for(src <- css)
                   yield <link href={src} media="all" rel="stylesheet" type="text/css"></link>
                 }
               </head>
               <body>
                 {body}
                 {
                   for(src <- js)
                   yield <script type="text/javascript" src={src}></script>
                 }
               </body>
             </html>)
    }
    //val plan = unfiltered.n
    val http = Http(port).handler(App).run { s => 
      val d= java.awt.Desktop.getDesktop
      d.browse(new java.net.URI(s"http://localhost:$port/"))
    }
  }
  
  
  
  def saveInitialData(db: ActorRef) = {
    import data.Location
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
/** Returns a file as an unfiltered response. */
case class FileResponse(file: java.io.File) extends ResponseStreamer {
  override def stream(os: java.io.OutputStream): Unit = {
     val in = new java.io.FileInputStream(file)
     val buf = new Array[Byte](64*1024)
     def read(): Unit = in read buf match {
       case -1 => ()
       case n =>
         os.write(buf, 0, n)
         read()
     }
     read()
  }
}


class EchoActor extends Actor {
  def receive: Receive = {
            case f: Function1[_,_] => 
              println(f.getClass)
              println(f.asInstanceOf[Function1[AnyRef, AnyRef]](null))
            case msg => println(msg)
          }
}