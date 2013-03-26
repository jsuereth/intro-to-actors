package debug

import akka.actor.Actor
import akka.actor.actorRef2Scala

case object PrintName

trait DebugActor extends Actor {
  def debugHandler: Receive = {
    case PrintName => 
      // TODO - Send this to the sender...
      sender ! context.self
      println(context.self.path.elements.mkString("/"))
      context.children foreach (_ ! PrintName)
  }
}