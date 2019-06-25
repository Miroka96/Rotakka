package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class GraphStoreSlave extends AbstractGraphStore {

    public static final String DEFAULT_NAME = "graphStoreSlave";

    public static Props props() {
        return Props.create(GraphStoreSlave.class);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class ShardToMove implements Serializable {
        public static final long serialVersionUID = 1;
        ActorRef previousOwner;
        int shardNumber;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class ShardsToMove implements Serializable {
        public static final long serialVersionUID = 1;
        ArrayList<ShardToMove> shards;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class RequestShard implements Serializable {
        public static final long serialVersionUID = 1;
        int shardNumber;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class ReceivedShard implements Serializable {
        public static final long serialVersionUID = 1;
        int shardNumber;
        SubGraph subGraph;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class DeleteShard implements Serializable {
        public static final long serialVersionUID = 1;
        int shardNumber;
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


    private HashMap<String, Vertex> vertices;

    @Override
    void add(Vertex vertex) {
        if (vertices.containsKey(vertex.key)) {
            merge(vertices.get(vertex.key), vertex);
        } else {
            vertices.put(vertex.key, vertex);
        }
    }

    private HashMap<String, Edge> edges;

    @Override
    void add(Edge edge) {
        if (edges.containsKey(edge.key)) {
            merge(edges.get(edge.key), edge);
        } else {
            edges.put(edge.key, edge);
        }
    }

}
