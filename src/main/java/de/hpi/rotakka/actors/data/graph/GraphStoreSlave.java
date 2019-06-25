package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.Edge;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.SubGraph;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.Vertex;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class GraphStoreSlave extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "graphStoreSlave";

    public static Props props() {
        return Props.create(GraphStoreSlave.class);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class AssignedShard implements Serializable {
        public static final long serialVersionUID = 1;
        ActorRef previousOwner;
        int shardNumber;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class AssignedShards implements Serializable {
        public static final long serialVersionUID = 1;
        ArrayList<AssignedShard> shards;
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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class ShardedEdge implements Serializable {
        public static final long serialVersionUID = 1L;
        int shardNumber;
        Edge edge;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class ShardedVertex implements Serializable {
        public static final long serialVersionUID = 1L;
        int shardNumber;
        Vertex vertex;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class ShardedSubGraph implements Serializable {
        public static final long serialVersionUID = 1L;
        int shardNumber;
        SubGraph subGraph;
    }


    static Vertex merge(Vertex into, Vertex from) {
        into.properties.putAll(from.properties);
        return into;
    }

    static Edge merge(Edge into, Edge from) {
        into.properties.putAll(from.properties);
        into.from = from.from;
        into.to = from.to;
        return into;
    }


    public static final class ShardSubGraph {
        HashMap<String, Vertex> vertices;
        HashMap<String, Edge> edges;
    }

    HashMap<Integer, ShardSubGraph> shards;

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ShardedSubGraph.class, this::add)
                .match(ShardedVertex.class, this::add)
                .match(ShardedEdge.class, this::add)
                .build();
    }

    @Override
    public void preStart() {
        GraphStoreMaster.getSingleton(context()).tell(new Messages.RegisterMe(), getSelf());
    }

    void add(ShardedVertex shardedVertex) {
        ShardSubGraph subGraph = shards.get(shardedVertex.shardNumber);
        assert subGraph != null;
        if (subGraph.vertices.containsKey(shardedVertex.vertex.key)) {
            merge(subGraph.vertices.get(shardedVertex.vertex.key), shardedVertex.vertex);
        } else {
            subGraph.vertices.put(shardedVertex.vertex.key, shardedVertex.vertex);
        }
    }

    void add(ShardedEdge shardedEdge) {
        ShardSubGraph subGraph = shards.get(shardedEdge.shardNumber);
        assert subGraph != null;
        if (subGraph.edges.containsKey(shardedEdge.edge.key)) {
            merge(subGraph.edges.get(shardedEdge.edge.key), shardedEdge.edge);
        } else {
            subGraph.edges.put(shardedEdge.edge.key, shardedEdge.edge);
        }
    }

    void add(ShardedSubGraph subGraph) {

    }

    void take(AssignedShards shards) {
        for (AssignedShard shard : shards.shards) {
            take(shard);
        }
    }

    void take(AssignedShard shard) {
        if (shard.previousOwner == null) {

        }
    }

}
