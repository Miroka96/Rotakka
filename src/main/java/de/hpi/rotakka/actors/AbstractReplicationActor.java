package de.hpi.rotakka.actors;

import akka.actor.ActorRef;
import akka.cluster.ddata.DistributedData;

public abstract class AbstractReplicationActor extends AbstractClusterActor {
    final ActorRef replicator = DistributedData.get(system).replicator();
}
