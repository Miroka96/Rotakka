package de.hpi.rotakka.actors;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;

public abstract class AbstractClusterActor extends AbstractLoggingActor {
    protected final ActorSystem system = getContext().system();
    protected final Cluster cluster = Cluster.get(system);
}
