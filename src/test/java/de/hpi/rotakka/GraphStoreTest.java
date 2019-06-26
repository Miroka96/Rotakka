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
    public void receiveShardedEdge() {
        new TestKit(system) {
            {
                ActorRef master = makeUsASlave(this, 1);

                GraphStoreMaster.Edge edge = new GraphStoreMaster.Edge("edge1", "nodea", "nodeb", null);
                master.tell(edge, getRef());
                expectMsg(new GraphStoreSlave.ShardedEdge(0, edge));

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
}
