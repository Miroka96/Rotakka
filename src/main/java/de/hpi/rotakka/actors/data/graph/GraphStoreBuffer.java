package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedEdge;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedSubGraph;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedVertex;
import de.hpi.rotakka.actors.data.graph.util.ExtendableSubGraph;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;

/*
https://sequencediagram.org/

title GraphStore

participant Master
participant Buffer
participant Slave
participant Slave2

Slave -> Master: RegisterMe
Master -> Slave: AssignedShards
opt
Slave -> Slave2: ShardRequest
Slave2 -> Master: StartBuffering
Master -> Buffer: StartBuffering
Buffer -> Slave2: StartedBuffering
Slave2 -> Slave: SentShard
end
Slave -> Master: CopiedShard
Master ->Buffer: StopBuffering(new target)
Buffer -> Slave: SubGraph
opt
Master -> Slave2: ShardToDelete
end
 */
public class GraphStoreBuffer extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "graphStoreBuffer";

    public static Props props(int shardNumber) {
        return Props.create(GraphStoreBuffer.class, shardNumber);
    }

    public static Props props(int shardNumber, ActorRef destination) {
        return Props.create(GraphStoreBuffer.class, shardNumber, destination);
    }

    private final int shardNumber;
    private ActorRef destination = null;
    private boolean buffering = false;

    GraphStoreBuffer(int shardNumber) {
        this(shardNumber, null);
    }

    GraphStoreBuffer(int shardNumber, ActorRef destination) {
        this.shardNumber = shardNumber;
        this.destination = destination;
    }

    @Data
    @NoArgsConstructor
    public static final class StartBuffering implements Serializable {
        public static final long serialVersionUID = 1;
    }

    @Data
    @NoArgsConstructor
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

    private void startBuffering(StartBuffering cmd) {
        this.buffering = true;
        bufferedShard = new ExtendableSubGraph();
    }

    private void stopBuffering(@NotNull StopBuffering cmd) {
        this.buffering = false;
        if (cmd.destination != null) {
            this.destination = cmd.destination;
        }

        GraphStoreMaster.SubGraph subGraph = bufferedShard.toSubGraph();
        if (subGraph != null) {
            ShardedSubGraph shardedSubGraph = new ShardedSubGraph(shardNumber, subGraph);
            forward(shardedSubGraph);
        }
        bufferedShard = null;
    }

    private void handle(@NotNull ShardedVertex shardedVertex) {
        if (buffering) {
            buffer(shardedVertex);
        } else {
            forward(shardedVertex);
        }
    }

    private void handle(@NotNull ShardedEdge shardedEdge) {
        if (buffering) {
            buffer(shardedEdge);
        } else {
            forward(shardedEdge);
        }
    }

    private void handle(@NotNull ShardedSubGraph shardedSubGraph) {
        if (buffering) {
            buffer(shardedSubGraph);
        } else {
            forward(shardedSubGraph);
        }
    }

    private ExtendableSubGraph bufferedShard = null;

    private void buffer(@NotNull ShardedVertex shardedVertex) {
        bufferedShard.vertices.add(shardedVertex.vertex);
    }

    private void buffer(@NotNull ShardedEdge shardedEdge) {
        bufferedShard.edges.add(shardedEdge.edge);
    }

    private void buffer(@NotNull ShardedSubGraph shardedSubGraph) {
        bufferedShard.vertices.addAll(Arrays.asList(shardedSubGraph.subGraph.vertices));
        bufferedShard.edges.addAll(Arrays.asList(shardedSubGraph.subGraph.edges));
    }

    private void forward(ShardedVertex shardedVertex) {
        destination.tell(shardedVertex, getSelf());
    }

    private void forward(ShardedEdge shardedEdge) {
        destination.tell(shardedEdge, getSelf());
    }

    private void forward(ShardedSubGraph shardedSubGraph) {
        destination.tell(shardedSubGraph, getSelf());
    }

}
