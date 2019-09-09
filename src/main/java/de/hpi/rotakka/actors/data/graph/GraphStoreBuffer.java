package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedEdge;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedSubGraph;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedVertex;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;

/*
https://sequencediagram.org/

title GraphStore

participant Master
participant Buffer1
participant Buffer2
participant Slave1
participant Slave2

Slave1 -> Master: RegisterMe
Master -> Slave1: AssignedShards
opt
Slave1 -> Slave2: ShardRequest
Slave2 -> Master: StartBuffering
Master -> Buffer1: StartBuffering
Buffer1 -> Slave1: StartedBuffering
Master -> Buffer2: StartBuffering
Buffer2 -> Slave2: StartedBuffering
Slave2 -> Slave1: SentShard
Slave1 ->Slave2: ReceivedShard
Slave1 -> Master: ShardReady
Slave2 -> Master: ShardReady
end
Master ->Buffer1: StopBuffering(new target)
Buffer1 -> Slave1: SubGraph
alt
Master ->Buffer2: StopBuffering(new target)
Buffer2 -> Slave2: SubGraph
else
Master -> Slave2: ShardToDelete
end
 */
public class GraphStoreBuffer extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "graphStoreBuffer";

    public static Props props(int shardNumber) {
        return props(shardNumber, null);
    }

    public static Props props(int shardNumber, ActorRef destination) {
        return Props.create(GraphStoreBuffer.class, shardNumber, destination);
    }

    private final int shardNumber;
    private ActorRef destination;
    private boolean buffering = false;

    GraphStoreBuffer(int shardNumber, ActorRef destination) {
        this.shardNumber = shardNumber;
        this.destination = destination;
    }

    @Data
    @NoArgsConstructor
    public static final class StartBuffering implements Serializable {
        public static final long serialVersionUID = 1;
        public ActorRef notify;
        public GraphStoreMaster.StartShardCopying originalRequest;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class StopBuffering implements Serializable {
        public static final long serialVersionUID = 1;
        ActorRef destination;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StartBuffering.class, this::startBuffering)
                .match(StopBuffering.class, this::stopBuffering)
                .match(ShardedSubGraph.class, this::handle)
                .match(ShardedVertex.class, this::handle)
                .match(ShardedEdge.class, this::handle)
                .build();
    }

    private void startBuffering(@NotNull StartBuffering cmd) {
        log.debug("Received start-buffering command for shard " + cmd.originalRequest.shardNumber +
                " and notifying " + cmd.notify.toString());
        this.buffering = true;
        if (bufferedShard == null) {
            bufferedShard = new GraphStoreMaster.ExtendableSubGraph();
        }
        cmd.notify.tell(new GraphStoreSlave.StartedBuffering(cmd.originalRequest), getSelf());
    }

    private void stopBuffering(@NotNull StopBuffering cmd) {
        StringBuilder sb = new StringBuilder();
        sb.append("Received stop-buffering command");
        this.buffering = false;
        if (cmd.destination != null) {
            this.destination = cmd.destination;
        }
        sb.append(" with destination ");
        sb.append(this.destination);
        if (bufferedShard != null) {
            GraphStoreMaster.SubGraph subGraph = bufferedShard.toSubGraph();
            if (subGraph != null) {
                ShardedSubGraph shardedSubGraph = new ShardedSubGraph(shardNumber, subGraph);
                forward(shardedSubGraph);
            }
            bufferedShard = null;
            sb.append(" and flushing buffer to destination");
        }
        log.debug(sb.toString());
    }

    private void handle(@NotNull ShardedVertex shardedVertex) {
        assert shardedVertex.vertex != null;
        assert shardedVertex.vertex.key != null;

        if (buffering) {
            buffer(shardedVertex);
        } else {
            if (destination != null) {
                forward(shardedVertex);
            } else {
                log.debug("Dropping received vertex " + shardedVertex.vertex.key + " - no destination configured");
            }
        }
    }

    private void handle(@NotNull ShardedEdge shardedEdge) {
        assert shardedEdge.edge != null;
        assert shardedEdge.edge.key != null;

        if (buffering) {
            buffer(shardedEdge);
        } else {
            if (destination != null) {
                forward(shardedEdge);
            } else {
                log.debug("Dropping received edge " + shardedEdge.edge.key + " - no destination configured");
            }
        }
    }

    private void handle(@NotNull ShardedSubGraph shardedSubGraph) {
        assert shardedSubGraph.subGraph != null;
        if (buffering) {
            buffer(shardedSubGraph);
        } else {
            if (destination != null) {
                forward(shardedSubGraph);
            } else {
                log.debug("Dropping received subgraph - no destination configured");
            }
        }
    }

    private GraphStoreMaster.ExtendableSubGraph bufferedShard = null;

    private void buffer(@NotNull ShardedVertex shardedVertex) {
        log.debug("Buffering received vertex " + shardedVertex.vertex.key);
        bufferedShard.vertices.add(shardedVertex.vertex);
    }

    private void buffer(@NotNull ShardedEdge shardedEdge) {
        log.debug("Buffering received edge " + shardedEdge.edge.key);
        bufferedShard.edges.add(shardedEdge.edge);
    }

    private void buffer(@NotNull ShardedSubGraph shardedSubGraph) {
        log.debug("Buffering received subgraph");
        bufferedShard.vertices.addAll(Arrays.asList(shardedSubGraph.subGraph.vertices));
        bufferedShard.edges.addAll(Arrays.asList(shardedSubGraph.subGraph.edges));
    }

    private void forward(@NotNull ShardedVertex shardedVertex) {
        log.debug("Forwarding received vertex " + shardedVertex.vertex.key);
        destination.tell(shardedVertex, getSelf());
    }

    private void forward(@NotNull ShardedEdge shardedEdge) {
        log.debug("Forwarding received edge " + shardedEdge.edge.key);
        destination.tell(shardedEdge, getSelf());
    }

    private void forward(@NotNull ShardedSubGraph shardedSubGraph) {
        log.debug("Forwarding received subgraph");
        destination.tell(shardedSubGraph, getSelf());
    }

}
