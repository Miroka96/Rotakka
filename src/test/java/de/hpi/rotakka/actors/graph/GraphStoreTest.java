package de.hpi.rotakka.actors.graph;


import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestProbe;
import akka.testkit.javadsl.TestKit;
import de.hpi.rotakka.actors.graph.GraphStoreMaster.ExtendableSubGraph;
import de.hpi.rotakka.actors.graph.GraphStoreMaster.SubGraph;
import de.hpi.rotakka.actors.graph.GraphStoreSlave.AssignedShards;
import de.hpi.rotakka.actors.graph.GraphStoreSlave.ShardedSubGraph;
import de.hpi.rotakka.actors.utils.Messages;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;
import scala.collection.JavaConverters;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

    @NotNull
    private ActorRef makeUsASlave(@NotNull ActorRef master, @NotNull TestProbe us, int shardCount) {
        return makeUsASlave(master, us, shardCount, null);
    }

    @Contract("_, _, _, _ -> param1")
    @NotNull
    private ActorRef makeUsASlave(@NotNull ActorRef master, @NotNull TestProbe us, int shardCount, ActorRef previousOwner) {
        // we make ourselves a slave
        master.tell(new Messages.RegisterMe(), us.ref());

        AssignedShards shardAssignment = new AssignedShards(new ArrayList<>());
        for (int shard = 0; shard < shardCount; shard++) {
            shardAssignment.getShards().add(new GraphStoreSlave.AssignedShard(previousOwner, shard));
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
    public void getShardAssignmentsTwoCopies() {
        new TestKit(system) {
            {
                ActorRef master = createMaster(1, 2);
                makeUsASlave(master, this, 1);

                TestProbe other = new TestProbe(system);
                makeUsASlave(master, other, 1, getRef());

                system.stop(master);
            }
        };
    }


    static class ForwardActor extends AbstractActor {
        final ActorRef destination;

        ForwardActor(ActorRef destination) {
            this.destination = destination;
        }

        static Props props(ActorRef destination) {
            return Props.create(ForwardActor.class, destination);
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder().matchAny(msg -> destination.tell(msg, getSender())).build();
        }
    }

    public void getRegisterMeFromSlave() {
        new TestKit(system) {
            {
                TestProbe master = new TestProbe(system); // lies in system namespace
                ActorRef forwarder = system.actorOf(ForwardActor.props(master.ref()), GraphStoreMaster.PROXY_NAME); // lies in user namespace

                ActorRef otherSlave = system.actorOf(GraphStoreSlave.props());
                master.expectMsgClass(Messages.RegisterMe.class);
                system.stop(otherSlave);
                system.stop(forwarder);
            }
        };
    }

    public void getShardRequestFromSlave() {
        new TestKit(system) {
            {
                TestProbe master = new TestProbe(system); // lies in system namespace
                ActorRef forwarder = system.actorOf(ForwardActor.props(master.ref()), GraphStoreMaster.PROXY_NAME); // lies in user namespace
                ActorRef otherSlave = system.actorOf(GraphStoreSlave.props());
                master.expectMsgClass(Messages.RegisterMe.class);

                TestProbe previousOwner = new TestProbe(system);
                otherSlave.tell(new GraphStoreSlave.AssignedShard(previousOwner.ref(), 0), forwarder);

                previousOwner.expectMsgClass(GraphStoreSlave.ShardRequest.class);
                assert (previousOwner.sender() == otherSlave);

                system.stop(otherSlave);
                system.stop(forwarder);
            }
        };
    }

    @Test
    public void testsWithUniqueMaster() {
        // tests must be run sequential to prevent following error:
        // akka.actor.InvalidActorNameException: actor name [graphStoreMasterProxy] is not unique!
        // uniqueness is required for real slaves to find the master

        getRegisterMeFromSlave();
        getShardRequestFromSlave();
        getShardRequestWithRealMaster();
        fullStoreWithInteractionAndLaterSlaves();
    }

    @Test
    public void getEmptyAssignedShards() {
        new TestKit(system) {
            {
                ActorRef master = createMaster(1, 1);
                TestProbe slave1 = new TestProbe(system);
                TestProbe slave2 = new TestProbe(system);

                makeUsASlave(master, slave1, 1, null);
                makeUsASlave(master, slave2, 0, slave1.ref());

                system.stop(master);
            }
        };
    }

    @Test
    public void getAssignedShardsWithPreviousOwner() {
        new TestKit(system) {
            {
                ActorRef master = createMaster(1, 2);
                TestProbe slave1 = new TestProbe(system, "slave1");
                TestProbe slave2 = new TestProbe(system, "slave2");

                makeUsASlave(master, slave1, 1, null);
                makeUsASlave(master, slave2, 1, slave1.ref());

                system.stop(master);
            }
        };
    }

    @Test
    public void getElementLocationsCopiedShard() {
        new TestKit(system) {
            {
                ActorRef master = createMaster(1, 2);
                TestProbe slave1 = new TestProbe(system);
                TestProbe slave2 = new TestProbe(system);

                makeUsASlave(master, slave1, 1, null);
                makeUsASlave(master, slave2, 1, slave1.ref());
                GraphStoreMaster.StartShardCopying copyCommand = new GraphStoreMaster.StartShardCopying(0, slave1.ref(), slave2.ref());
                master.tell(copyCommand, slave1.ref());
                slave1.expectMsg(new GraphStoreSlave.StartedBuffering(copyCommand));
                slave2.expectMsg(new GraphStoreSlave.StartedBuffering(copyCommand));
                master.tell(new GraphStoreMaster.ShardReady(0, slave2.ref(), slave1.ref()), slave2.ref());
                master.tell(new GraphStoreMaster.ShardReady(0, slave1.ref(), null), slave1.ref());

                GraphStoreMaster.Edge edge = new GraphStoreMaster.Edge("edge1", "nodea", "nodeb", null);
                master.tell(edge, getRef());

                List<java.lang.Class<? extends Serializable>> messages = new ArrayList<>();
                messages.add(GraphStoreSlave.ShardedEdge.class);
                messages.add(ShardedSubGraph.class);
                scala.collection.Seq<Class<? extends Serializable>> messageClasses =
                        JavaConverters
                                .asScalaBuffer(messages)
                                .toSeq();
                slave1.expectMsgAnyClassOf(messageClasses);
                slave2.expectMsgAnyClassOf(messageClasses);

                system.stop(master);
            }
        };
    }

    public void getShardRequestWithRealMaster() {
        new TestKit(system) {
            {
                ActorRef master = createMaster(1, 2);
                ActorRef forwarder = system.actorOf(ForwardActor.props(master), GraphStoreMaster.PROXY_NAME); // lies in user namespace
                TestProbe slave1 = new TestProbe(system);

                makeUsASlave(master, slave1, 1, null);

                ActorRef otherSlave = system.actorOf(GraphStoreSlave.props());
                slave1.expectMsgClass(GraphStoreSlave.ShardRequest.class);
                assert (slave1.lastSender() == otherSlave);

                system.stop(master);
                system.stop(forwarder);
                system.stop(otherSlave);
            }
        };
    }


    public void fullStoreWithInteractionAndLaterSlaves() {
        new TestKit(system) {
            {
                TestProbe us = new TestProbe(system);

                ActorRef master = createMaster(2, 2, true);
                ActorRef slave1 = system.actorOf(GraphStoreSlave.props(), "Slave1");
                ActorRef slave2 = system.actorOf(GraphStoreSlave.props(), "Slave2");

                GraphStoreMaster.Vertex v1 = new GraphStoreMaster.Vertex();
                v1.key = "v1";
                GraphStoreMaster.Vertex v2 = new GraphStoreMaster.Vertex();
                v2.key = "v2";
                GraphStoreMaster.Vertex v3 = new GraphStoreMaster.Vertex();
                v3.key = "v3";
                GraphStoreMaster.Vertex v4 = new GraphStoreMaster.Vertex();
                v4.key = "v4";

                GraphStoreMaster.Edge e1 = new GraphStoreMaster.Edge();
                e1.key = "e1";
                GraphStoreMaster.Edge e2 = new GraphStoreMaster.Edge();
                e2.key = "e2";
                GraphStoreMaster.Edge e3 = new GraphStoreMaster.Edge();
                e3.key = "e3";
                GraphStoreMaster.Edge e4 = new GraphStoreMaster.Edge();
                e4.key = "e4";

                master.tell(v1, us.ref());
                master.tell(e1, us.ref());
                master.tell(v2, us.ref());
                master.tell(e2, us.ref());

                us.expectNoMessage(FiniteDuration.apply(1, TimeUnit.SECONDS));

                ActorRef slave3 = system.actorOf(GraphStoreSlave.props(), "Slave3");
                ActorRef slave4 = system.actorOf(GraphStoreSlave.props(), "Slave4");

                master.tell(v3, us.ref());
                master.tell(e3, us.ref());
                master.tell(v4, us.ref());
                master.tell(e4, us.ref());

                us.expectNoMessage(FiniteDuration.apply(1, TimeUnit.SECONDS));

                system.stop(master);
                system.stop(slave1);
                system.stop(slave2);
                system.stop(slave3);
                system.stop(slave4);
            }
        };
    }
}
