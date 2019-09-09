package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.AssignedShard;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.AssignedShards;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedEdge;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedVertex;
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
        Map<String, Object> properties;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class Edge implements Serializable {
        public static final long serialVersionUID = 1L;
        String key;
        String from;
        String to;
        Map<String, Object> properties;
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
    public static final class StartShardCopying implements Serializable {
        public static final long serialVersionUID = 1;
        int shardNumber;
        ActorRef from;
        ActorRef to;
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
                .match(StartShardCopying.class, this::safeStartCopying)
                .match(ShardReady.class, this::enableShard)
                .match(RequestedEdgeLocation.class, this::get)
                .match(RequestedVertexLocation.class, this::get)
                .build();
    }

    private void add(@NotNull Vertex vertex) {
        ShardedVertex shardedVertex = new ShardedVertex(shardMapper.keyToShardNumber(vertex.key), vertex);
        log.debug("Received Vertex " + vertex.key + " and assigned it to shard " + shardedVertex.shardNumber
                + " - success: " + shardMapper.tellShard(shardedVertex.shardNumber, shardedVertex, getSelf()));
    }

    private void add(@NotNull Edge edge) {
        ShardedEdge shardedEdge = new ShardedEdge(shardMapper.keyToShardNumber(edge.key), edge);
        log.debug("Received Edge " + edge.key + " and assigned it to shard " + shardedEdge.shardNumber
                + " - success: " + shardMapper.tellShard(shardedEdge.shardNumber, shardedEdge, getSelf()));
    }

    public static class ExtendableSubGraph {
        ArrayList<GraphStoreMaster.Vertex> vertices = new ArrayList<>();
        ArrayList<GraphStoreMaster.Edge> edges = new ArrayList<>();

        public GraphStoreMaster.SubGraph toSubGraph() {
            GraphStoreMaster.Vertex[] newVertices = vertices.toArray(new GraphStoreMaster.Vertex[0]);
            GraphStoreMaster.Edge[] newEdges = edges.toArray(new GraphStoreMaster.Edge[0]);
            return new GraphStoreMaster.SubGraph(newVertices, newEdges);
        }
    }

    private void add(@NotNull SubGraph subGraph) {
        log.debug("Received Subgraph");
        // subgraph entities belong to different shards
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

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Integer, ExtendableSubGraph> entry : shards.entrySet()) {
            sb.append(", ");
            sb.append(entry.getKey());
            SubGraph subGraphByShard = entry.getValue().toSubGraph();
            GraphStoreSlave.ShardedSubGraph shardedSubGraph = new GraphStoreSlave.ShardedSubGraph(entry.getKey(), subGraphByShard);
            shardMapper.tellShard(entry.getKey(), shardedSubGraph, getSelf());
        }
        sb.delete(0, 2);
        log.debug("Split received subgraph into sharded subgraphs for shards: " + sb.toString());
    }

    private void add(Messages.RegisterMe slave) {
        add(getSender());
    }

    private void add(@NotNull ActorRef slave) {
        log.info("Adding " + slave.toString() + " as graph store slave");
        final ActorRef[] previousSlaves = shardMapper.getSlaves();
        shardMapper.add(slave);
        final int shardsPerSlave = Math.min(shardCount * duplicationLevel / (previousSlaves.length + 1), shardCount);

        ArrayList<AssignedShard> shardsToMove = new ArrayList<>(shardsPerSlave);
        HashSet<Integer> assignedShards = new HashSet<>(shardsPerSlave);

        // assign as much as possible
        int shardsLeft = shardsPerSlave - shardMapper.shardsUnassigned();
        for (int i = 0; i < shardsPerSlave && shardMapper.shardsUnassigned() > 0; i++) {
            int shard = shardMapper.assignShard(slave);
            ActorRef[] assignedSlaves = shardMapper.getSlaves(shard);
            ActorRef previousOwner = null;
            if (assignedSlaves.length > 1) {
                previousOwner = assignedSlaves[0];
            }
            if (previousOwner == slave) {
                previousOwner = assignedSlaves[1];
            }
            shardsToMove.add(new AssignedShard(previousOwner, shard));
        }

        if (shardsLeft > 0) {
            // start stealing
            HashMap<ActorRef, Iterator<Integer>> shardIterators = new HashMap<>(previousSlaves.length);
            for (ActorRef s : previousSlaves) {
                shardIterators.put(s, shardMapper.getShards(s));
            }

            for (Iterator<ActorRef> slaveIterator = Arrays.asList(previousSlaves).iterator();
                 shardsLeft > 0 && shardIterators.size() > 0;
                 shardsLeft--) {
                while (shardIterators.size() > 0) {
                    if (!slaveIterator.hasNext()) {
                        slaveIterator = Arrays.asList(previousSlaves).iterator();
                    }
                    ActorRef previousOwner = slaveIterator.next();
                    Iterator<Integer> shardIterator = shardIterators.get(previousOwner);

                    if (shardIterator == null) {
                        continue;
                    }

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
                        shardMapper.assign(slave, shard, false);
                        break;
                    }
                }
            }
        }
        for (AssignedShard shard : shardsToMove) {
            StringBuilder sb = new StringBuilder();
            sb.append("Assigning shard ");
            sb.append(shard.shardNumber);
            sb.append(" to slave ");
            sb.append(slave.toString());
            if (shard.previousOwner != null) {
                sb.append("; shard must be copied from ");
                sb.append(shard.previousOwner.toString());
            }
            log.debug(sb.toString());
        }
        slave.tell(new AssignedShards(shardsToMove), getSelf());
    }

    private Map<Integer, Map<ActorRef, Queue<StartShardCopying>>> bufferQueues = new HashMap<>();

    @NotNull
    private Queue<StartShardCopying> getLockingQueue(int shardNumber, @NotNull ActorRef actor) {
        Map<ActorRef, Queue<StartShardCopying>> queues = bufferQueues.computeIfAbsent(
                shardNumber,
                k -> new HashMap<>());

        Queue<StartShardCopying> lockingQueue = queues.computeIfAbsent(
                actor,
                ignore1 -> new LinkedList<>());

        return lockingQueue;
    }

    private void safeStartCopying(@NotNull StartShardCopying cmd) {
        assert getSender() == cmd.from;
        Queue<StartShardCopying> lockingQueue = getLockingQueue(cmd.shardNumber, cmd.from);
        lockingQueue.add(cmd);

        if (lockingQueue.size() > 1) {
            // already one ongoing copy procedure
            log.debug("Received shard copying command and put it into queue to process it later: " + cmd);
            return;
        }

        // no lock on the shard to copy from
        // necessary, because otherwise different copy instructions could like to
        // copy different states of the same shard which is currently not supported
        startCopying(cmd);
    }

    private void startCopying(@NotNull StartShardCopying cmd) {
        shardMapper.enableBuffer(cmd.shardNumber, cmd.from, cmd, getSelf());
        shardMapper.enableBuffer(cmd.shardNumber, cmd.to, cmd, getSelf());

        log.debug("Starting shard copying for shard " + cmd.shardNumber +
                ", to be copied from " + cmd.from.toString() + " to " + cmd.to.toString());
    }

    private void enableShard(@NotNull ShardReady msg) {
        assert msg.shardHolder != null;
        log.debug("Received " + msg);


        boolean successfullyDisabled = shardMapper.disableBuffer(msg.shardNumber, msg.shardHolder, getSelf());
        if (!successfullyDisabled) {
            log.debug("Shard " + msg.shardNumber + " could not be enabled on slave " +
                    msg.shardHolder + " - probably the shard has been deleted");
            return;
        }

        // buffer should now be flushed

        StringBuilder sb = new StringBuilder();
        sb.append("Enabling shard ");
        sb.append(msg.shardNumber);
        sb.append(" on slave ");
        sb.append(msg.shardHolder);
        if (msg.copiedFrom != null) {
            sb.append(" copied from ");
            sb.append(msg.copiedFrom);

            Queue<StartShardCopying> lockingQueue = getLockingQueue(msg.shardNumber, msg.copiedFrom);
            assert lockingQueue.size() > 0 : "Shard " + msg.shardNumber + " was not locked before copied from " +
                    msg.copiedFrom + " to " + msg.shardHolder;
            StartShardCopying executedCommand = lockingQueue.remove();
            assert executedCommand.to == msg.shardHolder :
                    "Removed wrong shard copy task from locking queue: " + executedCommand;

            StartShardCopying nextCommand = lockingQueue.peek();
            if (nextCommand != null) {
                startCopying(nextCommand);
                sb.append(" and started next copy operation");
            } else {
                if (shardMapper.getSlaves(msg.shardNumber).length > duplicationLevel) {
                    sb.append(" and told previous shard holder to delete shard");
                    msg.copiedFrom.tell(new GraphStoreSlave.ShardToDelete(msg.shardNumber), getSelf());
                    shardMapper.unassign(msg.copiedFrom, msg.shardNumber);
                }
            }
        }

        log.debug(sb.toString());
    }

    private void get(@NotNull RequestedVertexLocation vertex) {
        VertexLocation response = new VertexLocation(vertex.key, shardMapper.getSlaves(vertex.key));
        log.debug("Got location request for vertex " + vertex.key + " predicted on " + Arrays.toString(response.locations));
        getSender().tell(response, getSelf());
    }

    private void get(@NotNull RequestedEdgeLocation edge) {
        EdgeLocation response = new EdgeLocation(edge.key, shardMapper.getSlaves(edge.key));
        log.debug("Got location request for edge " + edge.key + " predicted on " + Arrays.toString(response.locations));
        getSender().tell(response, getSelf());
    }

}
