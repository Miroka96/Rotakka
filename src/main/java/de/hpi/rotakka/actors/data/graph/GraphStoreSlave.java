package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.Edge;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.SubGraph;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.Vertex;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
    public static final class StartedBuffering implements Serializable {
        public static final long serialVersionUID = 1;
        GraphStoreMaster.StartBufferings originalRequest;
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
    public static final class ReceivedShard implements Serializable {
        public static final long serialVersionUID = 1;
        int shardNumber;
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
        if (from.properties == null) return;
        if (into.properties == null) {
            into.properties = from.properties;
            return;
        }
        into.properties.putAll(from.properties);
    }

    private static void merge(@NotNull Edge into, @NotNull Edge from) {
        if (from.properties != null) {
            if (into.properties == null) {
                into.properties = from.properties;
            } else {
                into.properties.putAll(from.properties);
            }
        }
        if (from.from != null) {
            into.from = from.from;
        }
        if (from.to != null) {
            into.to = from.to;
        }
    }


    public static final class KeyedSubGraph {
        HashMap<String, Vertex> vertices = new HashMap<>();
        HashMap<String, Edge> edges = new HashMap<>();

        KeyedSubGraph() {
        }

        KeyedSubGraph(@NotNull SubGraph subGraph) {
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
        SubGraph toSubGraph() {
            Vertex[] newVertices = vertices.values().toArray(new Vertex[0]);
            Edge[] newEdges = edges.values().toArray(new Edge[0]);
            return new SubGraph(newVertices, newEdges);
        }
    }

    private HashMap<Integer, KeyedSubGraph> shards = new HashMap<>();

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ShardedSubGraph.class, this::add)
                .match(ShardedVertex.class, this::add)
                .match(ShardedEdge.class, this::add)
                .match(AssignedShards.class, this::take)
                .match(AssignedShard.class, this::take)
                .match(ShardRequest.class, this::answer)
                .match(StartedBuffering.class, this::react)
                .match(SentShard.class, this::receive)
                .match(ReceivedShard.class, this::react)
                .match(ShardToDelete.class, this::delete)
                .build();
    }

    private ActorSelection getMaster() {
        return GraphStoreMaster.getSingleton(context());
    }

    @Override
    public void preStart() {
        getMaster().tell(new Messages.RegisterMe(), getSelf());
    }

    private void add(@NotNull KeyedSubGraph subGraph, @NotNull Vertex vertex) {
        if (subGraph.vertices.containsKey(vertex.key)) {
            merge(subGraph.vertices.get(vertex.key), vertex);
        } else {
            subGraph.vertices.put(vertex.key, vertex);
        }
    }

    private void add(@NotNull ShardedVertex shardedVertex) {
        KeyedSubGraph subGraph = shards.get(shardedVertex.shardNumber);
        assert subGraph != null;
        add(subGraph, shardedVertex.vertex);
    }

    private void add(@NotNull KeyedSubGraph subGraph, @NotNull Edge edge) {
        if (subGraph.edges.containsKey(edge.key)) {
            merge(subGraph.edges.get(edge.key), edge);
        } else {
            subGraph.edges.put(edge.key, edge);
        }
    }

    private void add(@NotNull ShardedEdge shardedEdge) {
        KeyedSubGraph subGraph = shards.get(shardedEdge.shardNumber);
        assert subGraph != null;
        add(subGraph, shardedEdge.edge);
    }

    private void add(KeyedSubGraph keyedSubGraph, @NotNull SubGraph subGraph) {
        if (subGraph.vertices != null) {
            for (Vertex vertex : subGraph.vertices) {
                add(keyedSubGraph, vertex);
            }
        }
        if (subGraph.edges != null) {
            for (Edge edge : subGraph.edges) {
                add(keyedSubGraph, edge);
            }
        }
    }

    private void add(@NotNull ShardedSubGraph shardedSubGraph) {
        KeyedSubGraph subGraph = shards.get(shardedSubGraph.shardNumber);
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
            shards.put(shard.shardNumber, new KeyedSubGraph());
        } else {
            shard.previousOwner.tell(new ShardRequest(shard.shardNumber), getSelf());
        }
    }

    private void answer(@NotNull ShardRequest request) {
        getMaster().tell(
                new GraphStoreMaster.StartBufferings(
                        request.shardNumber,
                        new ActorRef[]{getSender(), getSelf()},
                        getSelf()
                ),
                getSelf()
        );
    }

    private void react(@NotNull StartedBuffering msg) {
        if (msg.originalRequest.requestedBy == getSelf()) {
            for (ActorRef otherSlave : msg.originalRequest.affectedShardHolders) {
                if (otherSlave == getSelf()) {
                    continue;
                }
                otherSlave.tell(
                        new SentShard(
                                msg.originalRequest.shardNumber,
                                shards.get(msg.originalRequest.shardNumber).toSubGraph()),
                        getSelf()
                );

            }
        }
    }

    private void enableOwnShard(int shardNumber) {
        enableOwnShard(shardNumber, null);
    }

    private void enableOwnShard(int shardNumber, ActorRef previousOwner) {
        getMaster().tell(
                new GraphStoreMaster.ShardReady(
                        shardNumber,
                        previousOwner,
                        getSelf()),
                getSelf());
    }

    private void receive(@NotNull SentShard shard) {
        shards.put(shard.shardNumber, new KeyedSubGraph(shard.subGraph));
        getSender().tell(new ReceivedShard(shard.shardNumber), getSelf());
        enableOwnShard(shard.shardNumber, getSender());
    }

    private void react(@NotNull ReceivedShard shard) {
        enableOwnShard(shard.shardNumber);
    }

    private void delete(@NotNull ShardToDelete shard) {
        shards.remove(shard.shardNumber);
    }
}
