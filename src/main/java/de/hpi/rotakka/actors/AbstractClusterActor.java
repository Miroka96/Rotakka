package de.hpi.rotakka.actors;

import akka.cluster.Cluster;

public abstract class AbstractClusterActor extends AbstractLoggingActor {
    protected final Cluster cluster = Cluster.get(getContext().system());
}
