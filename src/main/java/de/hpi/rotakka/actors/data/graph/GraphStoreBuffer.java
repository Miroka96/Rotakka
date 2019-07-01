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

public class GraphStoreBuffer extends AbstractLoggingActor {
    public static final String DEFAULT_NAME = "graphStoreBuffer";

    public static Props props(int shardNumber) {
        return Props.create(GraphStoreBuffer.class, shardNumber);
    }

    private final int shardNumber;
    private ActorRef destination;

    GraphStoreBuffer(int shardNumber) {
        this.shardNumber = shardNumber;
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class Destination implements Serializable {
        public static final long serialVersionUID = 1;
        ActorRef slave;
        boolean startForwarding;
    }

    @Data
    @NoArgsConstructor
    public static final class BufferCommand implements Serializable {
        public static final long serialVersionUID = 1;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class FlushCommand implements Serializable {
        public static final long serialVersionUID = 1;
        ActorRef slave;
        boolean continueForwarding;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Destination.class, this::setDestination)
                .match(BufferCommand.class, this::startBuffering)
                .match(GraphStoreSlave.ShardedSubGraph.class, this::buffer)
                .match(GraphStoreSlave.ShardedVertex.class, this::buffer)
                .match(GraphStoreSlave.ShardedEdge.class, this::buffer)
                .match(FlushCommand.class, this::flush)
                .build();
    }

    private void setDestination(Destination destination) {

    }

    private void startBuffering(BufferCommand cmd) {

    }

    private GraphStoreMaster.ExtendableSubGraph bufferedShard = new GraphStoreMaster.ExtendableSubGraph();

    private void buffer(@NotNull GraphStoreSlave.ShardedVertex shardedVertex) {
        bufferedShard.vertices.add(shardedVertex.vertex);
    }

    private void buffer(@NotNull GraphStoreSlave.ShardedEdge shardedEdge) {
        bufferedShard.edges.add(shardedEdge.edge);
    }

    private void buffer(@NotNull GraphStoreSlave.ShardedSubGraph shardedSubGraph) {
        bufferedShard.vertices.addAll(Arrays.asList(shardedSubGraph.subGraph.vertices));
        bufferedShard.edges.addAll(Arrays.asList(shardedSubGraph.subGraph.edges));
    }

    private void flush(FlushCommand destination) {
        GraphStoreMaster.SubGraph subGraph = bufferedShard.toSubGraph();
        if (subGraph != null) {
            GraphStoreSlave.ShardedSubGraph shardedSubGraph = new GraphStoreSlave.ShardedSubGraph(shardNumber, subGraph);
            destination.slave.tell(shardedSubGraph, getSelf());
        }
    }

    private void startForwarding() {

    }

    private void forward(GraphStoreSlave.ShardedVertex shardedVertex) {
        destination.tell(shardedVertex, getSelf());
    }

    private void forward(GraphStoreSlave.ShardedEdge shardedEdge) {
        destination.tell(shardedEdge, getSelf());
    }

    private void forward(GraphStoreSlave.ShardedSubGraph shardedSubGraph) {
        destination.tell(shardedSubGraph, getSelf());
    }

}
