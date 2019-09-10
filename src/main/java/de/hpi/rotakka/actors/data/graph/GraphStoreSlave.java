package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.Edge;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.SubGraph;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.Vertex;
import de.hpi.rotakka.actors.data.graph.util.GraphFileOutput;
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
        return props(null);
    }

    ActorRef notify;

    public static Props props(ActorRef notify) {
        return Props.create(GraphStoreSlave.class, notify);
    }

    public GraphStoreSlave(ActorRef notify) {
        this.notify = notify;
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
        GraphStoreMaster.StartShardCopying originalRequest;
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

    @NotNull
    @Contract("_, _ -> param1")
    private Vertex merge(@NotNull Vertex into, @NotNull Vertex from) {
        if (from.properties == null) return into;
        if (into.properties == null) {
            into.properties = from.properties;
            return into;
        }
        into.properties.putAll(from.properties);
        return into;
    }

    @NotNull
    @Contract("_, _ -> param1")
    private Edge merge(@NotNull Edge into, @NotNull Edge from) {
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
        return into;
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

    @Override
    public void postStop() {
        for (GraphFileOutput output : shardFiles.values()) {
            output.close();
        }
    }

    private HashMap<Integer, GraphFileOutput> shardFiles = new HashMap<>();

    @NotNull
    @Contract("_ -> new")
    private GraphFileOutput createGraphFileOutput(int shardNumber) {
        GraphFileOutput output = new GraphFileOutput(settings.graphStoreStoragePath, getSelf(), shardNumber, log);
        shardFiles.put(shardNumber, output);
        return output;
    }

    @NotNull
    private Vertex add(@NotNull KeyedSubGraph subGraph, @NotNull Vertex vertex) {
        if (subGraph.vertices.containsKey(vertex.key)) {
            return merge(subGraph.vertices.get(vertex.key), vertex);
        } else {
            subGraph.vertices.put(vertex.key, vertex);
            return vertex;
        }
    }

    private void add(@NotNull ShardedVertex shardedVertex) {
        log.debug("Adding vertex " + shardedVertex.vertex.key + " to shard " + shardedVertex.shardNumber);
        KeyedSubGraph subGraph = shards.get(shardedVertex.shardNumber);
        assert subGraph != null : "shard " + shardedVertex.shardNumber + " is not assigned to slave " + getSelf();
        shardFiles.get(shardedVertex.shardNumber).add(add(subGraph, shardedVertex.vertex));
    }

    @NotNull
    private Edge add(@NotNull KeyedSubGraph subGraph, @NotNull Edge edge) {
        if (subGraph.edges.containsKey(edge.key)) {
            return merge(subGraph.edges.get(edge.key), edge);
        } else {
            subGraph.edges.put(edge.key, edge);
            return edge;
        }
    }

    private void add(@NotNull ShardedEdge shardedEdge) {
        log.debug("Adding edge " + shardedEdge.edge.key + " to shard " + shardedEdge.shardNumber);
        KeyedSubGraph subGraph = shards.get(shardedEdge.shardNumber);
        assert subGraph != null : "shard " + shardedEdge.shardNumber + " is not assigned to slave " + getSelf();
        shardFiles.get(shardedEdge.shardNumber).add(add(subGraph, shardedEdge.edge));
    }

    @NotNull
    @Contract("_, _ -> param2")
    private SubGraph add(@NotNull KeyedSubGraph keyedSubGraph, @NotNull SubGraph subGraph) {
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
        return subGraph;
    }

    private void add(@NotNull ShardedSubGraph shardedSubGraph) {
        log.debug("Adding subgraph to shard " + shardedSubGraph.shardNumber);
        KeyedSubGraph subGraph = shards.get(shardedSubGraph.shardNumber);
        assert subGraph != null : "shard " + shardedSubGraph.shardNumber + " is not assigned to slave " + getSelf();
        shardFiles.get(shardedSubGraph.shardNumber).add(add(subGraph, shardedSubGraph.subGraph));
    }

    private void take(@NotNull AssignedShards shards) {
        log.debug("Received shard assignments");
        for (AssignedShard shard : shards.shards) {
            take(shard);
        }
    }

    private void take(@NotNull AssignedShard shard) {
        StringBuilder sb = new StringBuilder();
        sb.append("Shard ");
        sb.append(shard.shardNumber);
        sb.append(" has been assigned, ");
        if (shard.previousOwner == null) {
            shards.put(shard.shardNumber, new KeyedSubGraph());
            createGraphFileOutput(shard.shardNumber);
            sb.append("taking it");
        } else {
            shard.previousOwner.tell(new ShardRequest(shard.shardNumber), getSelf());
            sb.append("asking ");
            sb.append(shard.previousOwner);
            sb.append(" for a copy");
        }
        log.debug(sb.toString());
    }

    private void answer(@NotNull ShardRequest request) {
        log.debug("Received shard request from " + getSender() + " for shard " + request.shardNumber);
        getMaster().tell(
                new GraphStoreMaster.StartShardCopying(
                        request.shardNumber,
                        getSelf(),
                        getSender()
                ),
                getSelf()
        );
    }

    private void react(@NotNull StartedBuffering msg) {
        StringBuilder sb = new StringBuilder();
        sb.append("Received started-buffering notification for shard ")
                .append(msg.originalRequest.shardNumber);
        if (msg.originalRequest.from == getSelf()) {
            sb.append(" to copy it from ")
                    .append(msg.originalRequest.from)
                    .append(" to ")
                    .append(msg.originalRequest.to);

            assert shards.get(msg.originalRequest.shardNumber) != null : "shard " + msg.originalRequest.shardNumber + " does not/no longer exist at " + getSelf().toString();
            msg.originalRequest.to.tell(
                    new SentShard(
                            msg.originalRequest.shardNumber,
                            shards.get(msg.originalRequest.shardNumber).toSubGraph()),
                    getSelf()
            );

        } else {
            sb.append(", ignoring it");
        }
        log.debug(sb.toString());
    }

    private void enableOwnShard(int shardNumber) {
        enableOwnShard(shardNumber, null);
    }

    private void enableOwnShard(int shardNumber, ActorRef previousOwner) {
        getMaster().tell(
                new GraphStoreMaster.ShardReady(
                        shardNumber,
                        getSelf(),
                        previousOwner),
                getSelf());

    }

    private void receive(@NotNull SentShard shard) {
        log.debug("Received shard " + shard.shardNumber + " from " + getSender().toString());
        shards.put(shard.shardNumber, new KeyedSubGraph(shard.subGraph));

        GraphFileOutput output = createGraphFileOutput(shard.shardNumber);
        output.add(shard.subGraph);

        getSender().tell(new ReceivedShard(shard.shardNumber), getSelf());
        enableOwnShard(shard.shardNumber, getSender());
    }

    private void react(@NotNull ReceivedShard shard) {
        log.debug("Received receive acknowledgement for shard " + shard.shardNumber + " from " + getSender().toString());
        enableOwnShard(shard.shardNumber);
    }

    private void delete(@NotNull ShardToDelete shard) {
        log.debug("Received delete instruction for shard " + shard.shardNumber);
        shards.remove(shard.shardNumber);
        shardFiles.get(shard.shardNumber).close();
        shardFiles.get(shard.shardNumber).delete();
        shardFiles.remove(shard.shardNumber);
    }
}
