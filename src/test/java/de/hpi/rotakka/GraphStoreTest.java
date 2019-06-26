package de.hpi.rotakka;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave;
import de.hpi.rotakka.actors.utils.Messages;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

import java.util.ArrayList;
import java.util.Collections;

public class GraphStoreTest extends JUnitSuite {
    public static final long serialVersionUID = 1;

    private static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    private ActorRef makeUsASlave(@NotNull TestKit us, int shardCount) {
        final ActorRef master = system.actorOf(GraphStoreMaster.props(shardCount, 1));

        // we make ourselves a slave
        master.tell(new Messages.RegisterMe(), us.getRef());

        GraphStoreSlave.AssignedShards shardAssignment = new GraphStoreSlave.AssignedShards(new ArrayList<>());
        for (int shard = 0; shard < shardCount; shard++) {
            shardAssignment.getShards().add(new GraphStoreSlave.AssignedShard(null, shard));
        }
        us.expectMsg(shardAssignment);
        return master;
    }

    @Test
    public void receiveShardedElementsOneShard() {
        new TestKit(system) {
            {
                ActorRef master = makeUsASlave(this, 1);

                GraphStoreMaster.Edge edge = new GraphStoreMaster.Edge("edge1", "nodea", "nodeb", null);
                master.tell(edge, getRef());
                expectMsg(new GraphStoreSlave.ShardedEdge(0, edge));

                GraphStoreMaster.Vertex vertex = new GraphStoreMaster.Vertex("vertex1", null);
                master.tell(vertex, getRef());
                expectMsg(new GraphStoreSlave.ShardedVertex(0, vertex));

                GraphStoreMaster.ExtendableSubGraph extendableSubGraph = new GraphStoreMaster.ExtendableSubGraph();
                extendableSubGraph.vertices.add(vertex);
                extendableSubGraph.edges.add(edge);
                GraphStoreMaster.SubGraph subGraph = extendableSubGraph.toSubGraph();
                master.tell(subGraph, getRef());
                expectMsg(new GraphStoreSlave.ShardedSubGraph(0, subGraph));
            }
        };
    }

    @Test
    public void receiveShardedElementsTwoShards() {
        new TestKit(system) {
            {
                ActorRef master = makeUsASlave(this, 2);

                GraphStoreMaster.Edge edge1 = new GraphStoreMaster.Edge("edge1", "nodea", "nodeb", null);
                master.tell(edge1, getRef());
                expectMsg(new GraphStoreSlave.ShardedEdge(0, edge1));

                GraphStoreMaster.Edge edge2 = new GraphStoreMaster.Edge("edge2", "nodea", "nodeb", null);
                master.tell(edge2, getRef());
                expectMsg(new GraphStoreSlave.ShardedEdge(1, edge2));

                GraphStoreMaster.Vertex vertex1 = new GraphStoreMaster.Vertex("vertex", null);
                master.tell(vertex1, getRef());
                expectMsg(new GraphStoreSlave.ShardedVertex(0, vertex1));

                GraphStoreMaster.Vertex vertex2 = new GraphStoreMaster.Vertex("vertex1", null);
                master.tell(vertex2, getRef());
                expectMsg(new GraphStoreSlave.ShardedVertex(1, vertex2));

                GraphStoreMaster.ExtendableSubGraph extendableSubGraph1 = new GraphStoreMaster.ExtendableSubGraph();
                GraphStoreMaster.ExtendableSubGraph extendableSubGraph2 = new GraphStoreMaster.ExtendableSubGraph();
                extendableSubGraph1.vertices.add(vertex1);
                extendableSubGraph2.vertices.add(vertex2);
                extendableSubGraph1.edges.add(edge1);
                extendableSubGraph2.edges.add(edge2);
                GraphStoreMaster.SubGraph subGraph1 = extendableSubGraph1.toSubGraph();
                GraphStoreMaster.SubGraph subGraph2 = extendableSubGraph2.toSubGraph();
                master.tell(subGraph1, getRef());
                master.tell(subGraph2, getRef());

                GraphStoreSlave.ShardedSubGraph shardedSubGraph1 = new GraphStoreSlave.ShardedSubGraph(0, subGraph1);
                GraphStoreSlave.ShardedSubGraph shardedSubGraph2 = new GraphStoreSlave.ShardedSubGraph(1, subGraph2);

                expectMsgAllOf(shardedSubGraph1, shardedSubGraph2);
            }
        };
    }

    @Test
    public void getElementLocations() {
        new TestKit(system) {
            {
                ActorRef master = makeUsASlave(this, 1);

                GraphStoreMaster.Edge edge = new GraphStoreMaster.Edge("edge1", "nodea", "nodeb", null);
                master.tell(edge, getRef());
                expectMsgClass(GraphStoreSlave.ShardedEdge.class);

                master.tell(new GraphStoreMaster.RequestedEdgeLocation("edge1"), getRef());
                expectMsg(new GraphStoreMaster.EdgeLocation("edge1", Collections.singletonList(getRef()).toArray(new ActorRef[0])));

                GraphStoreMaster.Vertex vertex = new GraphStoreMaster.Vertex("vertex1", null);
                master.tell(vertex, getRef());
                expectMsgClass(GraphStoreSlave.ShardedVertex.class);

                master.tell(new GraphStoreMaster.RequestedVertexLocation("vertex1"), getRef());
                expectMsg(new GraphStoreMaster.VertexLocation("vertex1", Collections.singletonList(getRef()).toArray(new ActorRef[0])));
            }
        };
    }
}
