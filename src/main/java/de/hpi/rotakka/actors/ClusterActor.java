package de.hpi.rotakka.actors;

import akka.cluster.Cluster;

public abstract class ClusterActor extends LoggingActor {
    protected final Cluster cluster = Cluster.get(getContext().system());
}
