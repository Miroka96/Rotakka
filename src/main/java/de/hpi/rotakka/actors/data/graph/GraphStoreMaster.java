package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class GraphStoreMaster extends AbstractGraphStore {

    public static final String DEFAULT_NAME = "graphStoreMaster";

    public static Props props() {
        return Props.create(GraphStoreMaster.class);
    }

    @Data
    @AllArgsConstructor
    public static final class IAmYourSlave implements Serializable {
        public static final long serialVersionUID = 1L;

        ActorRef slave;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubGraph.class, this::add)
                .match(Vertex.class, this::add)
                .match(Edge.class, this::add)
                .match(IAmYourSlave.class, this::add)
                .build();
    }

    @Override
    void add(Vertex vertex) {

    }

    @Override
    void add(Edge edge) {

    }

    void add(IAmYourSlave slave) {
        add(slave.slave);
    }

    void add(ActorRef slave) {

    }

}
