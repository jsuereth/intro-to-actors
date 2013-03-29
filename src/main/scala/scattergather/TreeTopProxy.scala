package scattergather

import akka.actor.{ Address, Actor, ActorRef, RootActorPath }
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.{LeaderChanged, CurrentClusterState}

class TreeTopProxy extends Actor with debug.DebugActor {
  // subscribe to RoleLeaderChanged, re-subscribe when restart
  override def preStart(): Unit =
    Cluster(context.system).subscribe(self, classOf[LeaderChanged])
  override def postStop(): Unit =
    Cluster(context.system).unsubscribe(self)
  var leaderAddress: Option[Address] = None
 
  def receive = debugHandler orElse {
    case state: CurrentClusterState   => leaderAddress = state.leader
    case LeaderChanged(leader)        => leaderAddress = leader
    case other                        => consumer foreach { _ forward other }
  }
 
  def consumer: Option[ActorRef] =
    leaderAddress map (a ⇒ context.actorFor(RootActorPath(a) /
      "user" / "singleton" / "search-tree"))
  
  override def debugChildren =
    (consumer.toSeq ++ context.children)
}