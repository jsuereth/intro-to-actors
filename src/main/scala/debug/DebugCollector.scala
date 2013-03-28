package debug

import akka.actor._
import akka.util.Timeout
import java.util.concurrent.TimeUnit
import scala.concurrent.Future

object DebugCollector {
  
  def collectGraph(system: ActorSystem, topLevels: Seq[ActorRef]): Future[java.io.File] = {
    import akka.pattern.ask
    import system.dispatcher
    implicit val timeout = Timeout(3, TimeUnit.SECONDS)
    val collector = system.actorOf(Props[DebugCollectorActor])
    for(actor <- topLevels)
      actor.tell(GetName, collector)
      
    (collector ? GetPng).mapTo[java.io.File]
  }
}

case object GetPng
class DebugCollectorActor extends Actor {
  val defaultTimeout = Timeout(2, TimeUnit.SECONDS)
  var listener: ActorRef = self
  var actors: Seq[ActorRef] = Seq.empty
  
  def receive: Receive = {
    case GetPng =>
      listener = sender
    case ref: ActorRef => 
      actors +:= ref
      context setReceiveTimeout defaultTimeout.duration
    case ReceiveTimeout => 
      generateGraph()
      context stop self
  }
  // Specify our default timeout immediately.
  context setReceiveTimeout defaultTimeout.duration
  
  def generateGraph(): Unit = {
    import graph._
    object actorGraph extends Graph[ActorRef, Nothing] {
      val nodes: Set[Node[ActorRef]] = (actors map SimpleNode.apply).toSet 
      def edges(n: Node[ActorRef]): Seq[Edge[ActorRef, Nothing]] = {
        for {
          n2 <- nodes.toSeq
          if n2.value != n.value
          if n2.value.path.elements == n.value.path.elements.dropRight(1)
        } yield EmptyEdge[ActorRef](n, n2)
      }
    }
    
    val dotString = Graphs.toDotFile(actorGraph)(_.path.name)
    val tmp = java.io.File.createTempFile("dot", "png")
    tmp.deleteOnExit()
    makeDotImage(tmp, dotString)
    listener ! tmp
  }
  
  def makeAndOpenDotImage(contents: String): Unit = {
    val tmp = java.io.File.createTempFile("dot", "png")
    tmp.deleteOnExit
    makeDotImage(tmp, contents)
    val d = java.awt.Desktop.getDesktop
    d.open(tmp)
  }
  
  def makeDotImage(file: java.io.File, contents: String): Unit = {
    val tmp = java.io.File.createTempFile("dot", "graph")
    writeFile(tmp, contents)
    import sys.process._
    Process(Seq("dot", "-Tpng", s"-o${file.getAbsolutePath}", tmp.getAbsolutePath)).! match {
      case 0 => ()
      case n => System.err.println("Failed to save png.")
    }
    tmp.delete()
  }
  
  def writeFile(file: java.io.File, contents: String): Unit = {
    val in = new java.io.FileWriter(file)
    try in.write(contents)
    finally in.close()
  }
}