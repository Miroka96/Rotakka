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
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

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
    public static final class ShardRequest implements Serializable {
        public static final long serialVersionUID = 1;
        int shardNumber;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class SentShard implements Serializable {
        public static final long serialVersionUID = 1;
        int shardNumber;
        SubGraph subGraph;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class ShardToDelete implements Serializable {
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


    private static void merge(@NotNull Vertex into, @NotNull Vertex from) {
        into.properties.putAll(from.properties);
    }

    private static void merge(@NotNull Edge into, @NotNull Edge from) {
        into.properties.putAll(from.properties);
        into.from = from.from;
        into.to = from.to;
    }


    public static final class ShardSubGraph {
        HashMap<String, Vertex> vertices = new HashMap<>();
        HashMap<String, Edge> edges = new HashMap<>();

        ShardSubGraph() {
        }

        ShardSubGraph(@NotNull SubGraph subGraph) {
            if (subGraph.vertices != null) {
                for (Vertex vertex : subGraph.vertices) {
                    vertices.put(vertex.key, vertex);
                }
            }
            if (subGraph.edges != null) {
                for (Edge edge : subGraph.edges) {
                    edges.put(edge.key, edge);
                }
            }
        }

        @NotNull
        @Contract(" -> new")
        SubGraph toSubGraph() {
            return new SubGraph(
                    vertices.values().toArray(new Vertex[0]),
                    edges.values().toArray(new Edge[0])
            );
        }
    }

    private HashMap<Integer, ShardSubGraph> shards = new HashMap<>();

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ShardedSubGraph.class, this::add)
                .match(ShardedVertex.class, this::add)
                .match(ShardedEdge.class, this::add)
                .match(AssignedShards.class, this::take)
                .match(AssignedShard.class, this::take)
                .match(ShardRequest.class, this::answer)
                .match(SentShard.class, this::receive)
                .match(ShardToDelete.class, this::delete)
                .build();
    }

    @Override
    public void preStart() {
        GraphStoreMaster.getSingleton(context()).tell(new Messages.RegisterMe(), getSelf());
    }

    private void add(@NotNull ShardSubGraph subGraph, @NotNull Vertex vertex) {
        if (subGraph.vertices.containsKey(vertex.key)) {
            merge(subGraph.vertices.get(vertex.key), vertex);
        } else {
            subGraph.vertices.put(vertex.key, vertex);
        }
    }

    private void add(@NotNull ShardedVertex shardedVertex) {
        ShardSubGraph subGraph = shards.get(shardedVertex.shardNumber);
        assert subGraph != null;
        add(subGraph, shardedVertex.vertex);
    }

    private void add(@NotNull ShardSubGraph subGraph, @NotNull Edge edge) {
        if (subGraph.edges.containsKey(edge.key)) {
            merge(subGraph.edges.get(edge.key), edge);
        } else {
            subGraph.edges.put(edge.key, edge);
        }
    }

    private void add(@NotNull ShardedEdge shardedEdge) {
        ShardSubGraph subGraph = shards.get(shardedEdge.shardNumber);
        assert subGraph != null;
        add(subGraph, shardedEdge.edge);
    }

    private void add(ShardSubGraph shardSubGraph, @NotNull SubGraph subGraph) {
        if (subGraph.vertices != null) {
            for (Vertex vertex : subGraph.vertices) {
                add(shardSubGraph, vertex);
            }
        }
        if (subGraph.edges != null) {
            for (Edge edge : subGraph.edges) {
                add(shardSubGraph, edge);
            }
        }
    }

    private void add(@NotNull ShardedSubGraph shardedSubGraph) {
        ShardSubGraph subGraph = shards.get(shardedSubGraph.shardNumber);
        assert subGraph != null;
        add(subGraph, shardedSubGraph.subGraph);
    }

    private void take(@NotNull AssignedShards shards) {
        for (AssignedShard shard : shards.shards) {
            take(shard);
        }
    }

    private void take(@NotNull AssignedShard shard) {
        if (shard.previousOwner == null) {
            shards.put(shard.shardNumber, new ShardSubGraph());
        } else {
            getSender().tell(new ShardRequest(shard.shardNumber), getSelf());
        }
    }

    private void answer(@NotNull ShardRequest request) {
        getSender().tell(
                new SentShard(
                        request.shardNumber,
                        shards.get(request.shardNumber).toSubGraph()),
                getSelf());
    }

    private void receive(@NotNull SentShard shard) {
        shards.put(shard.shardNumber, new ShardSubGraph(shard.subGraph));
        GraphStoreMaster.getSingleton(context()).tell(
                new GraphStoreMaster.MovedShard(
                        shard.shardNumber,
                        getSender(),
                        getSelf()),
                getSelf());
    }

    private void delete(@NotNull ShardToDelete shard) {
        shards.remove(shard.shardNumber);
    }
}
