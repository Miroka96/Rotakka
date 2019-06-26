package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.*;

public class GraphStoreMaster extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "graphStoreMaster";
    public static final String PROXY_NAME = DEFAULT_NAME + "Proxy";

    public static final int DEFAULT_SHARD_COUNT = 2 * 3 * 4 * 5;
    private final int shardCount;

    public static final int DEFAULT_DUPLICATION_LEVEL = 2;
    private final int duplicationLevel;

    public static ActorSelection getSingleton(@NotNull akka.actor.ActorContext context) {
        return context.actorSelection("/user/" + PROXY_NAME);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class Vertex implements Serializable {
        public static final long serialVersionUID = 1L;
        String key;
        HashMap<String, Object> properties;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class Edge implements Serializable {
        public static final long serialVersionUID = 1L;
        String key;
        String from;
        String to;
        HashMap<String, Object> properties;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class SubGraph implements Serializable {
        public static final long serialVersionUID = 1L;
        Vertex[] vertices;
        Edge[] edges;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class MovedShard implements Serializable {
        public static final long serialVersionUID = 1;
        int id;
        ActorRef previousOwner;
        ActorRef newOwner;
    }

    public GraphStoreMaster() {
        this(DEFAULT_SHARD_COUNT, DEFAULT_DUPLICATION_LEVEL);
    }

    private Queue<Integer> shardCopiesToAssign = new LinkedList<Integer>();

    public GraphStoreMaster(int shardCount, int duplicationLevel) {
        this.shardCount = shardCount;
        this.duplicationLevel = duplicationLevel;

        shardToSlaves = new ArrayList<>(shardCount);
        for (int shard = 0; shard < shardCount; shard++) {
            shardToSlaves.add(new HashSet<>(duplicationLevel));
        }

        for (int copy = 0; copy < duplicationLevel; copy++) {
            for (int shard = 0; shard < shardCount; shard++) {
                shardCopiesToAssign.add(shard);
            }
        }
    }

    public static Props props() {
        return Props.create(GraphStoreMaster.class);
    }

    public static Props props(int shardCount, int duplicationLevel) {
        return Props.create(GraphStoreMaster.class, shardCount, duplicationLevel);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubGraph.class, this::add)
                .match(Vertex.class, this::add)
                .match(Edge.class, this::add)
                .match(Messages.RegisterMe.class, this::add)
                .match(MovedShard.class, this::moveShard)
                .build();
    }

    private ArrayList<HashSet<ActorRef>> shardToSlaves;
    private HashMap<ActorRef, HashSet<Integer>> slaveToShards = new HashMap<>(5);

    @Contract(pure = true)
    private int keyToShard(@NotNull final String key) {
        return key.hashCode() % shardCount;
    }

    private void add(Vertex vertex) {
        GraphStoreSlave.ShardedVertex shardedVertex = new GraphStoreSlave.ShardedVertex(keyToShard(vertex.key), vertex);
        shardToSlaves.get(shardedVertex.shardNumber).forEach(slave -> slave.tell(shardedVertex, getSelf()));
    }

    private void add(Edge edge) {
        GraphStoreSlave.ShardedEdge shardedEdge = new GraphStoreSlave.ShardedEdge(keyToShard(edge.key), edge);
        shardToSlaves.get(shardedEdge.shardNumber).forEach(slave -> slave.tell(shardedEdge, getSelf()));
    }

    public static final class ExtendableSubGraph {
        public ArrayList<Vertex> vertices = new ArrayList<>();
        public ArrayList<Edge> edges = new ArrayList<>();

        @Nullable
        public SubGraph toSubGraph() {
            Vertex[] vs = null;
            Edge[] es = null;
            if (vertices.size() > 0) {
                vs = vertices.toArray(new Vertex[0]);
            }
            if (edges.size() > 0) {
                es = edges.toArray(new Edge[0]);
            }
            if (vs == null && es == null) return null;
            return new SubGraph(vs, es);
        }
    }

    private void add(@NotNull SubGraph subGraph) {
        HashMap<Integer, ExtendableSubGraph> shards = new HashMap<>();
        if (subGraph.vertices != null) {
            for (Vertex vertex : subGraph.vertices) {
                int shardNumber = keyToShard(vertex.key);
                ExtendableSubGraph extendableSubGraph = shards.get(shardNumber);
                if (extendableSubGraph == null) {
                    extendableSubGraph = new ExtendableSubGraph();
                    shards.put(shardNumber, extendableSubGraph);
                }
                extendableSubGraph.vertices.add(vertex);
            }
        }
        if (subGraph.edges != null) {
            for (Edge edge : subGraph.edges) {
                int shardNumber = keyToShard(edge.key);
                ExtendableSubGraph extendableSubGraph = shards.get(shardNumber);
                if (extendableSubGraph == null) {
                    extendableSubGraph = new ExtendableSubGraph();
                    shards.put(shardNumber, extendableSubGraph);
                }
                extendableSubGraph.edges.add(edge);
            }
        }

        for (Map.Entry<Integer, ExtendableSubGraph> entry : shards.entrySet()) {
            SubGraph subGraphByShard = entry.getValue().toSubGraph();
            if (subGraphByShard != null) {
                GraphStoreSlave.ShardedSubGraph shardedSubGraph = new GraphStoreSlave.ShardedSubGraph(entry.getKey(), subGraphByShard);
                shardToSlaves.get(entry.getKey()).forEach(slave -> slave.tell(shardedSubGraph, getSelf()));
            }
        }
    }

    private ArrayList<ActorRef> slaves = new ArrayList<>();


    private void add(Messages.RegisterMe slave) {
        add(getSender());
    }

    private void add(ActorRef slave) {
        slaves.add(slave);
        slaveToShards.put(slave, new HashSet<>());
        int shardsPerSlave = Math.min(shardCount * duplicationLevel / slaves.size(), shardCount);

        ArrayList<GraphStoreSlave.AssignedShard> shardsToMove = new ArrayList<>(shardsPerSlave);
        HashSet<Integer> assignedShards = new HashSet<>(shardsPerSlave);

        if (shardCopiesToAssign.size() >= shardsPerSlave) {
            for (int i = 0; i < shardsPerSlave; i++) {
                int shard = shardCopiesToAssign.remove();
                assign(slave, shard);
                shardsToMove.add(new GraphStoreSlave.AssignedShard(null, shard));
            }
        } else {
            for (int shard : shardCopiesToAssign) {
                assign(slave, shard);
                assignedShards.add(shard);
                shardsToMove.add(new GraphStoreSlave.AssignedShard(null, shard));

                shardsPerSlave--;
            }
            shardCopiesToAssign.clear();

            Iterator<ActorRef> slaveIterator = slaves.iterator();
            HashMap<ActorRef, Iterator<Integer>> shardIterators = new HashMap<>(slaves.size());
            for (ActorRef s : slaves) {
                shardIterators.put(s, slaveToShards.get(s).iterator());
            }

            for (; shardsPerSlave > 0 && shardIterators.size() > 0; shardsPerSlave--) {
                while (shardIterators.size() > 0) {
                    if (!slaveIterator.hasNext()) {
                        slaveIterator = slaves.iterator();
                    }
                    ActorRef previousOwner = slaveIterator.next();

                    if (!shardIterators.get(previousOwner).hasNext()) {
                        shardIterators.remove(previousOwner);
                        continue;
                    }

                    int shard = shardIterators.get(previousOwner).next();

                    // cannot reassign same shard to slave
                    while (assignedShards.contains(shard) && shardIterators.get(previousOwner).hasNext()) {
                        shard = shardIterators.get(previousOwner).next();
                    }

                    if (!assignedShards.contains(shard)) {
                        shardsToMove.add(new GraphStoreSlave.AssignedShard(previousOwner, shard));
                        assignedShards.add(shard);
                        break;
                    }
                }
            }
        }

        slave.tell(new GraphStoreSlave.AssignedShards(shardsToMove), getSelf());
    }

    private void assign(ActorRef slave, int shard) {
        slaveToShards.get(slave).add(shard);
        shardToSlaves.get(shard).add(slave);
    }

    private void unassign(ActorRef slave, int shard) {
        slaveToShards.get(slave).remove(shard);
        shardToSlaves.get(shard).remove(slave);
    }

    private void moveShard(@NotNull MovedShard shard) {
        // TODO during shard movement, all messages to the moving copy should be buffered
        unassign(shard.previousOwner, shard.id);
        assign(shard.newOwner, shard.id);
        shard.previousOwner.tell(new GraphStoreSlave.ShardToDelete(shard.id), getSelf());
    }

}
