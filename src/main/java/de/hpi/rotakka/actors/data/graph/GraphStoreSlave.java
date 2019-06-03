package de.hpi.rotakka.actors.data.graph;

import akka.actor.Props;

public class GraphStoreSlave extends AbstractGraphStore {

    public static final String DEFAULT_NAME = "graphStoreSlave";

    public static Props props() {
        return Props.create(GraphStoreSlave.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubGraph.class, this::add)
                .match(Vertex.class, this::add)
                .match(Edge.class, this::add)
                .build();
    }

    @Override
    void add(Vertex vertex) {

    }

    @Override
    void add(Edge edge) {

    }

}
