package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.AssignedShard;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.AssignedShards;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedEdge;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedVertex;
import de.hpi.rotakka.actors.data.graph.util.ExtendableSubGraph;
import de.hpi.rotakka.actors.data.graph.util.ShardMapper;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.*;

public class GraphStoreMaster extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "graphStoreMaster";
    public static final String PROXY_NAME = DEFAULT_NAME + "Proxy";

    public static final int DEFAULT_SHARD_COUNT = 2 * 3 * 2 * 5 * 7 * 2; // creates even splits for up to 8 servers
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
    public static final class StartBufferings implements Serializable {
        public static final long serialVersionUID = 1;
        int shardNumber;
        ActorRef[] affectedShardHolders;
        ActorRef requestedBy;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class ShardReady implements Serializable {
        public static final long serialVersionUID = 1;
        public int shardNumber;
        public ActorRef shardHolder;
        public ActorRef copiedFrom; // optional
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class RequestedVertexLocation implements Serializable {
        public static final long serialVersionUID = 1L;
        String key;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class VertexLocation implements Serializable {
        public static final long serialVersionUID = 1L;
        String key;
        ActorRef[] locations;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class RequestedEdgeLocation implements Serializable {
        public static final long serialVersionUID = 1L;
        String key;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class EdgeLocation implements Serializable {
        public static final long serialVersionUID = 1L;
        String key;
        ActorRef[] locations;
    }

    /*
        @Data
        @NoArgsConstructor
        public static final class StartBuffering implements Serializable {
            public static final long serialVersionUID = 1;
            ActorRef slave;
            int shardNumber;
        }
    */

    private final ShardMapper shardMapper;

    public static Props props() {
        return Props.create(GraphStoreMaster.class);
    }

    public static Props props(int shardCount, int duplicationLevel) {
        return Props.create(GraphStoreMaster.class, shardCount, duplicationLevel);
    }

    public GraphStoreMaster() {
        this(DEFAULT_SHARD_COUNT, DEFAULT_DUPLICATION_LEVEL);
    }

    public GraphStoreMaster(int shardCount, int duplicationLevel) {
        this.shardCount = shardCount;
        this.duplicationLevel = duplicationLevel;
        this.shardMapper = new ShardMapper(shardCount, duplicationLevel, context());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubGraph.class, this::add)
                .match(Vertex.class, this::add)
                .match(Edge.class, this::add)
                .match(Messages.RegisterMe.class, this::add)
                //.match(StartBuffering.class, this::startBuffering)
                .match(StartBufferings.class, this::startBuffering)
                .match(ShardReady.class, this::enableShard)
                .match(RequestedEdgeLocation.class, this::get)
                .match(RequestedVertexLocation.class, this::get)
                .build();
    }


    private void add(Vertex vertex) {
        ShardedVertex shardedVertex = new ShardedVertex(shardMapper.keyToShardNumber(vertex.key), vertex);
        shardMapper.tellShard(shardedVertex.shardNumber, shardedVertex, getSelf());
    }

    private void add(Edge edge) {
        ShardedEdge shardedEdge = new ShardedEdge(shardMapper.keyToShardNumber(edge.key), edge);
        shardMapper.tellShard(shardedEdge.shardNumber, shardedEdge, getSelf());
    }

    private void add(@NotNull SubGraph subGraph) {
        HashMap<Integer, ExtendableSubGraph> shards = new HashMap<>();
        if (subGraph.vertices != null) {
            for (Vertex vertex : subGraph.vertices) {
                int shardNumber = shardMapper.keyToShardNumber(vertex.key);
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
                int shardNumber = shardMapper.keyToShardNumber(edge.key);
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
                shardMapper.tellShard(entry.getKey(), shardedSubGraph, getSelf());
            }
        }
    }

    private void add(Messages.RegisterMe slave) {
        add(getSender());
    }

    private void add(ActorRef slave) {
        shardMapper.add(slave);
        final Set<ActorRef> slaves = shardMapper.getSlaves();
        final int shardsPerSlave = Math.min(shardCount * duplicationLevel / slaves.size(), shardCount);

        ArrayList<AssignedShard> shardsToMove = new ArrayList<>(shardsPerSlave);
        HashSet<Integer> assignedShards = new HashSet<>(shardsPerSlave);

        // assign as much as possible
        int shardsLeft = shardsPerSlave - shardMapper.shardsUnassigned();
        for (int i = 0; i < shardsPerSlave && shardMapper.shardsUnassigned() > 0; i++) {
            int shard = shardMapper.assignShard(slave);
            shardsToMove.add(new AssignedShard(null, shard));
        }

        if (shardsLeft > 0) {
            // start stealing
            HashMap<ActorRef, Iterator<Integer>> shardIterators = new HashMap<>(slaves.size());
            for (ActorRef s : slaves) {
                shardIterators.put(s, shardMapper.getShards(s));
            }

            for (Iterator<ActorRef> slaveIterator = slaves.iterator();
                 shardsLeft > 0 && shardIterators.size() > 0;
                 shardsLeft--) {
                while (shardIterators.size() > 0) {
                    if (!slaveIterator.hasNext()) {
                        slaveIterator = slaves.iterator();
                    }
                    ActorRef previousOwner = slaveIterator.next();
                    Iterator<Integer> shardIterator = shardIterators.get(previousOwner);

                    if (!shardIterator.hasNext()) {
                        shardIterators.remove(previousOwner);
                        continue;
                    }

                    int shard = shardIterator.next();

                    // cannot reassign same shard to slave
                    while (assignedShards.contains(shard) && shardIterator.hasNext()) {
                        shard = shardIterator.next();
                    }

                    if (!assignedShards.contains(shard)) {
                        shardsToMove.add(new AssignedShard(previousOwner, shard));
                        assignedShards.add(shard);
                        break;
                    }
                }
            }
        }

        slave.tell(new AssignedShards(shardsToMove), getSelf());
    }

    private void startBuffering(@NotNull StartBufferings cmds) {
        for (ActorRef slave : cmds.affectedShardHolders) {
            GraphStoreBuffer.StartBuffering start = new GraphStoreBuffer.StartBuffering();
            start.notify = slave;
            start.originalRequest = cmds;
            shardMapper.tellBuffer(cmds.shardNumber, slave, start, getSelf());
        }
    }

    private void enableShard(@NotNull ShardReady msg) {
        shardMapper.enableShard(msg.shardNumber, msg.shardHolder, getSelf());
    }

    private void get(@NotNull RequestedVertexLocation vertex) {
        getSender().tell(
                new VertexLocation(
                        vertex.key,
                        shardMapper.getSlaves(vertex.key)),
                getSelf());
    }

    private void get(@NotNull RequestedEdgeLocation edge) {
        getSender().tell(
                new EdgeLocation(
                        edge.key,
                        shardMapper.getSlaves(edge.key)),
                getSelf());
    }

}
