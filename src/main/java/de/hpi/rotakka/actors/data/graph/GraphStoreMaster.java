package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rotakka.actors.utils.RegisterMe;

public class GraphStoreMaster extends AbstractGraphStore {

    public static final String DEFAULT_NAME = "graphStoreMaster";

    public static Props props() {
        return Props.create(GraphStoreMaster.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubGraph.class, this::add)
                .match(Vertex.class, this::add)
                .match(Edge.class, this::add)
                .match(RegisterMe.class, this::add)
                .build();
    }

    @Override
    void add(Vertex vertex) {

    }

    @Override
    void add(Edge edge) {

    }

    void add(RegisterMe slave) {
        add(getSender());
    }

    void add(ActorRef slave) {

    }

}
