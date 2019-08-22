package de.hpi.rotakka;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestProbe;
import akka.testkit.javadsl.TestKit;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster.SubGraph;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.AssignedShards;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave.ShardedSubGraph;
import de.hpi.rotakka.actors.data.graph.util.ExtendableSubGraph;
import de.hpi.rotakka.actors.utils.Messages;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;
import scala.collection.JavaConverters;

import java.util.ArrayList;

public class GraphStoreTest extends JUnitSuite {
    public static final long serialVersionUID = 1;

    private static ActorSystem system;
    private static ActorRef master;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    private static ActorRef createMaster(int shardCount, int duplicationLevel) {
        return createMaster(shardCount, duplicationLevel, false);
    }

    private static ActorRef createMaster(int shardCount, int duplicationLevel, boolean unique) {
        // unique must be true to enable slaves to find the master
        if (unique) {
            return system.actorOf(GraphStoreMaster.props(shardCount, duplicationLevel), GraphStoreMaster.PROXY_NAME);
        } else {
            return system.actorOf(GraphStoreMaster.props(shardCount, duplicationLevel));
        }

    }

    @NotNull
    private ActorRef makeUsASlave(@NotNull TestKit us, int shardCount) {
        return makeUsASlave(createMaster(shardCount, 1), us, shardCount);
    }

    @Contract("_, _, _ -> param1")
    @NotNull
    private ActorRef makeUsASlave(@NotNull ActorRef master, @NotNull TestKit us, int shardCount) {
        // we make ourselves a slave
        master.tell(new Messages.RegisterMe(), us.getRef());

        AssignedShards shardAssignment = new AssignedShards(new ArrayList<>());
        for (int shard = 0; shard < shardCount; shard++) {
            shardAssignment.getShards().add(new GraphStoreSlave.AssignedShard(null, shard));
        }
        us.expectMsg(shardAssignment);
        return master;
    }

    @Contract("_, _, _ -> param1")
    @NotNull
    private ActorRef makeUsASlave(@NotNull ActorRef master, @NotNull TestProbe us, int shardCount) {
        // we make ourselves a slave
        master.tell(new Messages.RegisterMe(), us.ref());

        AssignedShards shardAssignment = new AssignedShards(new ArrayList<>());
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

                ExtendableSubGraph extendableSubGraph = new ExtendableSubGraph();
                extendableSubGraph.vertices.add(vertex);
                extendableSubGraph.edges.add(edge);
                SubGraph subGraph = extendableSubGraph.toSubGraph();
                master.tell(subGraph, getRef());
                expectMsg(new ShardedSubGraph(0, subGraph));

                system.stop(master);
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

                ExtendableSubGraph extendableSubGraph1 = new ExtendableSubGraph();
                ExtendableSubGraph extendableSubGraph2 = new ExtendableSubGraph();
                extendableSubGraph1.vertices.add(vertex1);
                extendableSubGraph2.vertices.add(vertex2);
                extendableSubGraph1.edges.add(edge1);
                extendableSubGraph2.edges.add(edge2);
                SubGraph subGraph1 = extendableSubGraph1.toSubGraph();
                SubGraph subGraph2 = extendableSubGraph2.toSubGraph();
                master.tell(subGraph1, getRef());
                master.tell(subGraph2, getRef());

                ShardedSubGraph shardedSubGraph1 = new ShardedSubGraph(0, subGraph1);
                ShardedSubGraph shardedSubGraph2 = new ShardedSubGraph(1, subGraph2);

                expectMsgAllOf(shardedSubGraph1, shardedSubGraph2);

                system.stop(master);
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
                expectMsg(new GraphStoreMaster.EdgeLocation("edge1", new ActorRef[]{getRef()}));

                GraphStoreMaster.Vertex vertex = new GraphStoreMaster.Vertex("vertex1", null);
                master.tell(vertex, getRef());
                expectMsgClass(GraphStoreSlave.ShardedVertex.class);

                master.tell(new GraphStoreMaster.RequestedVertexLocation("vertex1"), getRef());
                expectMsg(new GraphStoreMaster.VertexLocation("vertex1", new ActorRef[]{getRef()}));

                system.stop(master);
            }
        };
    }


    @Test
    public void getShardAssignmentsTwoCopies() throws InterruptedException {
        new TestKit(system) {
            {
                ActorRef master = createMaster(1, 2, true);
                makeUsASlave(master, this, 1);

                TestProbe other = new TestProbe(system);
                makeUsASlave(master, other, 1);

                system.stop(master);
            }
        };
    }

    @Test
    public void getElementLocationsCopiedShard() throws InterruptedException {
        new TestKit(system) {
            {
                ActorRef master = createMaster(1, 2, true);
                makeUsASlave(master, this, 1);


                GraphStoreMaster.Edge edge = new GraphStoreMaster.Edge("edge1", "nodea", "nodeb", null);
                master.tell(edge, getRef());
                expectMsgClass(GraphStoreSlave.ShardedEdge.class);

                master.tell(new GraphStoreMaster.RequestedEdgeLocation("edge1"), getRef());
                expectMsg(new GraphStoreMaster.EdgeLocation("edge1", new ActorRef[]{getRef()}));

                ActorRef otherSlave = system.actorOf(GraphStoreSlave.props());
                TestProbe locationQuestioneer = new TestProbe(system);
                awaitAssert(() -> {
                    master.tell(new GraphStoreMaster.RequestedEdgeLocation("edge1"), locationQuestioneer.ref());
                    ArrayList<Object> expectedMessages = new ArrayList<>(2);
                    expectedMessages.add(new GraphStoreMaster.EdgeLocation("edge1", new ActorRef[]{getRef(), otherSlave}));
                    expectedMessages.add(new GraphStoreMaster.EdgeLocation("edge1", new ActorRef[]{otherSlave, getRef()}));

                    locationQuestioneer.expectMsgAnyOf(JavaConverters.asScalaBuffer(expectedMessages).toSeq());
                    return true;
                });

                expectMsgClass(GraphStoreSlave.ShardRequest.class);
                assert (getLastSender() == otherSlave);


/*
                GraphStoreMaster.Vertex vertex = new GraphStoreMaster.Vertex("vertex1", null);
                master.tell(vertex, getRef());
                expectMsg(new GraphStoreSlave.ShardedVertex(0, vertex));

                ExtendableSubGraph extendableSubGraph = new ExtendableSubGraph();
                extendableSubGraph.vertices.add(vertex);
                extendableSubGraph.edges.add(edge);
                SubGraph subGraph = extendableSubGraph.toSubGraph();
                master.tell(subGraph, getRef());
                expectMsg(new ShardedSubGraph(0, subGraph));

 */
                system.stop(master);
                system.stop(otherSlave);
            }
        };
    }

}
