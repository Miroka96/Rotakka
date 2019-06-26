package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class GraphStoreBuffer extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "graphStoreBuffer";

    public static Props props() {
        return Props.create(GraphStoreBuffer.class);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class FlushingDestination implements Serializable {
        public static final long serialVersionUID = 1;
        ActorRef slave;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(GraphStoreSlave.ShardedSubGraph.class, this::add)
                .match(GraphStoreSlave.ShardedVertex.class, this::add)
                .match(GraphStoreSlave.ShardedEdge.class, this::add)
                .match(FlushingDestination.class, this::flushBuffer)
                .build();
    }

    private HashMap<Integer, GraphStoreMaster.ExtendableSubGraph> shards = new HashMap<>();

    private void add(@NotNull GraphStoreSlave.ShardedVertex shardedVertex) {
        GraphStoreMaster.ExtendableSubGraph extendableSubGraph = shards.get(shardedVertex.shardNumber);
        if (extendableSubGraph == null) {
            extendableSubGraph = new GraphStoreMaster.ExtendableSubGraph();
            shards.put(shardedVertex.shardNumber, extendableSubGraph);
        }
        extendableSubGraph.vertices.add(shardedVertex.vertex);

    }

    private void add(@NotNull GraphStoreSlave.ShardedEdge shardedEdge) {
        GraphStoreMaster.ExtendableSubGraph extendableSubGraph = shards.get(shardedEdge.shardNumber);
        if (extendableSubGraph == null) {
            extendableSubGraph = new GraphStoreMaster.ExtendableSubGraph();
            shards.put(shardedEdge.shardNumber, extendableSubGraph);
        }
        extendableSubGraph.edges.add(shardedEdge.edge);
    }

    private void add(@NotNull GraphStoreSlave.ShardedSubGraph shardedSubGraph) {
        GraphStoreMaster.ExtendableSubGraph extendableSubGraph = shards.get(shardedSubGraph.shardNumber);
        if (extendableSubGraph == null) {
            extendableSubGraph = new GraphStoreMaster.ExtendableSubGraph();
            shards.put(shardedSubGraph.shardNumber, extendableSubGraph);
        }
        extendableSubGraph.vertices.addAll(Arrays.asList(shardedSubGraph.subGraph.vertices));
        extendableSubGraph.edges.addAll(Arrays.asList(shardedSubGraph.subGraph.edges));
    }

    private void flushBuffer(FlushingDestination destination) {
        for (Map.Entry<Integer, GraphStoreMaster.ExtendableSubGraph> entry : shards.entrySet()) {
            GraphStoreMaster.SubGraph subGraphByShard = entry.getValue().toSubGraph();
            if (subGraphByShard != null) {
                GraphStoreSlave.ShardedSubGraph shardedSubGraph = new GraphStoreSlave.ShardedSubGraph(entry.getKey(), subGraphByShard);
                destination.slave.tell(shardedSubGraph, getSelf());
            }
        }
    }
}
