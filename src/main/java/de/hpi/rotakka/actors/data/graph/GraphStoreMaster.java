package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;

public class GraphStoreMaster extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "graphStoreMaster";
    public static final String PROXY_NAME = DEFAULT_NAME + "Proxy";

    public static final int DEFAULT_SHARD_COUNT = 2 * 3 * 4 * 5;
    private final int shardCount;

    public static final int DEFAULT_DUPLICATION_LEVEL = 2;
    private final int duplicationLevel;

    public static ActorSelection getSingleton(akka.actor.ActorContext context) {
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
    public static final class MovedShard implements Serializable {
        public static final long serialVersionUID = 1;
        int id;
        ActorRef newOwner;
    }

    public GraphStoreMaster() {
        this(DEFAULT_SHARD_COUNT, DEFAULT_DUPLICATION_LEVEL);
    }

    private Queue<Integer> shardCopiesToAssign = new LinkedList<Integer>();

    public GraphStoreMaster(int shardCount, int duplicationLevel) {
        this.shardCount = shardCount;
        this.duplicationLevel = duplicationLevel;

        shardToSlaves = new ArrayList<>(shardCount);
        for (int shard = 0; shard < shardCount; shard++) {
            shardToSlaves.add(new HashSet<>(duplicationLevel));
        }

        for (int copy = 0; copy < duplicationLevel; copy++) {
            for (int shard = 0; shard < shardCount; shard++) {
                shardCopiesToAssign.add(shard);
            }
        }

    }

    public static Props props() {
        return Props.create(GraphStoreMaster.class);
    }

    public static Props props(int shardCount, int duplicationLevel) {
        return Props.create(GraphStoreMaster.class, shardCount, duplicationLevel);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubGraph.class, this::add)
                .match(Vertex.class, this::add)
                .match(Edge.class, this::add)
                .match(Messages.RegisterMe.class, this::add)
                .build();
    }

    private ArrayList<HashSet<ActorRef>> shardToSlaves;
    private HashMap<ActorRef, HashSet<Integer>> slaveToShards = new HashMap<>(5);

    void add(Vertex vertex) {

    }

    void add(Edge edge) {

    }

    void add(SubGraph subGraph) {
        if (subGraph.vertices != null) {
            for (Vertex vertex : subGraph.vertices) {
                add(vertex);
            }
        }
        if (subGraph.edges != null) {
            for (Edge edge : subGraph.edges) {
                add(edge);
            }
        }
    }

    private ArrayList<ActorRef> slaves = new ArrayList<>();


    void add(Messages.RegisterMe slave) {
        add(getSender());
    }

    void add(ActorRef slave) {
        slaves.add(slave);
        slaveToShards.put(slave, new HashSet<>());
        int shardsPerSlave = Math.min(shardCount * duplicationLevel / slaves.size(), shardCount);

        ArrayList<GraphStoreSlave.AssignedShard> shardsToMove = new ArrayList<>(shardsPerSlave);
        HashSet<Integer> assignedShards = new HashSet<>(shardsPerSlave);

        if (shardCopiesToAssign.size() >= shardsPerSlave) {
            for (int i = 0; i < shardsPerSlave; i++) {
                int shard = shardCopiesToAssign.remove();
                assign(slave, shard);
                shardsToMove.add(new GraphStoreSlave.AssignedShard(null, shard));
            }
        } else {
            for (int shard : shardCopiesToAssign) {
                assign(slave, shard);
                assignedShards.add(shard);
                shardsToMove.add(new GraphStoreSlave.AssignedShard(null, shard));

                shardsPerSlave--;
            }
            shardCopiesToAssign.clear();

            Iterator<ActorRef> slaveIterator = slaves.iterator();
            HashMap<ActorRef, Iterator<Integer>> shardIterators = new HashMap<>(slaves.size());
            for (ActorRef s : slaves) {
                shardIterators.put(s, slaveToShards.get(s).iterator());
            }

            for (; shardsPerSlave > 0 && shardIterators.size() > 0; shardsPerSlave--) {
                while (shardIterators.size() > 0) {
                    if (!slaveIterator.hasNext()) {
                        slaveIterator = slaves.iterator();
                    }
                    ActorRef previousOwner = slaveIterator.next();

                    if (!shardIterators.get(previousOwner).hasNext()) {
                        shardIterators.remove(previousOwner);
                        continue;
                    }

                    int shard = shardIterators.get(previousOwner).next();

                    // cannot reassign same shard to slave
                    while (assignedShards.contains(shard) && shardIterators.get(previousOwner).hasNext()) {
                        shard = shardIterators.get(previousOwner).next();
                    }

                    if (!assignedShards.contains(shard)) {
                        shardsToMove.add(new GraphStoreSlave.AssignedShard(previousOwner, shard));
                        assignedShards.add(shard);
                        break;
                    }
                }
            }
        }

        slave.tell(new GraphStoreSlave.AssignedShards(shardsToMove), getSelf());
    }

    void assign(ActorRef slave, int shard) {
        slaveToShards.get(slave).add(shard);
        shardToSlaves.get(shard).add(slave);
    }

}
