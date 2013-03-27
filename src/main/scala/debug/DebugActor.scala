package debug

import akka.actor.Actor
import akka.actor.actorRef2Scala

case object GetName

trait DebugActor extends Actor {
  def debugHandler: Receive = {
    case GetName => 
      // TODO - Aggregate this krap and make a nice tree.
      sender ! context.self
      context.children foreach (_.tell(GetName, sender))
  }
}