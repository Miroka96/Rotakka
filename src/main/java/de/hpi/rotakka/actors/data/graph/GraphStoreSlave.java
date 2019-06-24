package de.hpi.rotakka.actors.data.graph;

import akka.actor.Props;
import de.hpi.rotakka.actors.utils.Messages;

import java.util.HashMap;

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
    public void preStart() {
        GraphStoreMaster.getSingleton(context()).tell(new Messages.RegisterMe(), getSelf());
    }


    HashMap<String, Vertex> vertices;

    @Override
    void add(Vertex vertex) {
        if (vertices.containsKey(vertex.key)) {
            merge(vertices.get(vertex.key), vertex);
        } else {
            vertices.put(vertex.key, vertex);
        }
    }

    HashMap<String, Edge> edges;

    @Override
    void add(Edge edge) {
        if (edges.containsKey(edge.key)) {
            merge(edges.get(edge.key), edge);
        } else {
            edges.put(edge.key, edge);
        }
    }

}
