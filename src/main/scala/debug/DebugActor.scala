package debug

import akka.actor.Actor
import akka.actor.actorRef2Scala

case object PrintName

trait DebugActor extends Actor {
  def debugHandler: Receive = {
    case PrintName => 
      // TODO - Aggregate this krap and make a nice tree.
      sender ! context.self
      println(context.self.path)
      context.children foreach (_ ! PrintName)
  }
}