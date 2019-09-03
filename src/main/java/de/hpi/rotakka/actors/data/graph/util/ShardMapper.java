package de.hpi.rotakka.actors.data.graph.util;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import akka.japi.Pair;
import de.hpi.rotakka.actors.data.graph.GraphStoreBuffer;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.*;

public class ShardMapper {

    private final int shardCount;
    private final int duplicationLevel;
    private final ActorContext context;
    private Queue<Integer> shardCopiesToAssign = new LinkedList<>();

    // shard -> slave -> buffer
    private ArrayList<HashMap<ActorRef, ActorRef>> shardToSlaves;
    private ArrayList<Queue<Pair<Object, ActorRef>>> shardMessageBuffers; // as long as no buffer is registered
    private HashMap<ActorRef, HashSet<Integer>> slaveToShards = new HashMap<>(5);

    public ShardMapper(int shardCount, int duplicationLevel, ActorContext context) {
        this.shardCount = shardCount;
        this.duplicationLevel = duplicationLevel;
        this.context = context;

        shardToSlaves = new ArrayList<>(shardCount);
        for (int shard = 0; shard < shardCount; shard++) {
            shardToSlaves.add(new HashMap<>(duplicationLevel));
        }

        shardMessageBuffers = new ArrayList<>(shardCount);
        for (int shard = 0; shard < shardCount; shard++) {
            shardMessageBuffers.add(new LinkedList<>());
        }

        for (int copy = 0; copy < duplicationLevel; copy++) {
            for (int shard = 0; shard < shardCount; shard++) {
                shardCopiesToAssign.add(shard);
            }
        }
    }

    @Contract(pure = true)
    public int keyToShardNumber(@NotNull final String key) {
        int mod = key.hashCode() % shardCount;
        if (mod < 0) {
            mod += shardCount;
        }
        return mod;
    }

    private void tellBuffer(int shardNumber, @NotNull ActorRef slave, @NotNull Object msg, ActorRef sender) {
        HashMap<ActorRef, ActorRef> bufferMap = shardToSlaves.get(shardNumber);
        assert bufferMap != null : "SlaveToBuffer Map not yet created for shard " + shardNumber;
        ActorRef buffer = bufferMap.get(slave);
        assert buffer != null : "No buffer available for shard " + shardNumber + " and slave " + slave.toString();
        buffer.tell(msg, sender);
    }

    public void tellShard(int shardNumber, Object msg, ActorRef sender) {
        if (shardToSlaves.get(shardNumber).size() == 0) {
            shardMessageBuffers.get(shardNumber).add(Pair.create(msg, sender));
            return;
        }

        for (Pair<Object, ActorRef> p : shardMessageBuffers.get(shardNumber)) {
            shardToSlaves.get(shardNumber).forEach((slave, buffer) -> buffer.tell(p.first(), p.second()));
        }
        shardToSlaves.get(shardNumber).forEach((slave, buffer) -> buffer.tell(msg, sender));
    }


    public void assign(@NotNull ActorRef slave, int shard, boolean ready) {
        String slaveName = slave.toString();
        int slashIndex = slaveName.lastIndexOf('/');
        int hashIndex = slaveName.lastIndexOf('#');
        slaveName = slaveName.substring(slashIndex + 1, hashIndex);
        String bufferName = GraphStoreBuffer.DEFAULT_NAME + "_" + shard + "_" + slaveName;
        ActorRef buffer;
        if (ready) {
            buffer = context.actorOf(GraphStoreBuffer.props(shard, slave), bufferName);
        } else {
            buffer = context.actorOf(GraphStoreBuffer.props(shard), bufferName);
        }
        slaveToShards.putIfAbsent(slave, new HashSet<>());
        slaveToShards.get(slave).add(shard);
        shardToSlaves.get(shard).put(slave, buffer);
    }

    public void unassign(@NotNull ActorRef slave, int shard) {
        ActorRef buffer = shardToSlaves.get(shard).get(slave);
        assert buffer != null : "buffer for shard " + shard + " and slave " + slave + " does not/no longer exist";
        slaveToShards.get(slave).remove(shard);
        shardToSlaves.get(shard).remove(slave);
        context.stop(buffer);
    }

    Map<Integer, Map<ActorRef, Integer>> bufferLocks = new HashMap<>();

    public void enableBuffer(int shardNumber, @NotNull ActorRef slave, @NotNull GraphStoreMaster.StartBufferings cmds, @NotNull ActorRef sender) {
        // increase a counter for every buffer
        Map<ActorRef, Integer> slaves = bufferLocks.getOrDefault(shardNumber, new HashMap<>());
        int locks = slaves.getOrDefault(slave, 0);
        slaves.put(slave, locks + 1);
        bufferLocks.put(shardNumber, slaves);

        GraphStoreBuffer.StartBuffering start = new GraphStoreBuffer.StartBuffering();
        start.notify = slave;
        start.originalRequest = cmds;
        tellBuffer(shardNumber, slave, start, sender);
    }

    // returns true if shard is safe to be deleted
    public boolean disableBuffer(int shardNumber, @NotNull ActorRef slave, @NotNull ActorRef sender) {
        // decrease a counter for every buffer
        Map<ActorRef, Integer> slaves = bufferLocks.get(shardNumber);
        assert slaves != null : "buffer for shard " + shardNumber + " and slave " + slave.toString() + " was not enabled previously";
        int locks = slaves.get(slave);
        assert locks > 0 : "buffer for shard " + shardNumber + " and slave " + slave.toString() + " was not locked previously";

        locks--;
        if (locks > 0) {
            slaves.put(slave, locks);
            bufferLocks.put(shardNumber, slaves);
        } else {
            slaves.remove(slave);
        }

        if (locks == 0) {
            // buffer no longer needed
            tellBuffer(shardNumber, slave, new GraphStoreBuffer.StopBuffering(slave), sender);
            return true;
        } else {
            return false;
        }
    }

    public ActorRef[] getSlaves() {
        return slaveToShards.keySet().toArray(new ActorRef[0]);
    }

    public ActorRef[] getSlaves(int shardNumber) {
        return shardToSlaves.get(shardNumber).keySet().toArray(new ActorRef[0]);
    }

    public ActorRef[] getSlaves(String objectKey) {
        return shardToSlaves.get(keyToShardNumber(objectKey)).keySet().toArray(new ActorRef[0]);
    }

    public Iterator<Integer> getShards(ActorRef slave) {
        return slaveToShards.get(slave).iterator();
    }

    public void add(ActorRef slave) {
        slaveToShards.put(slave, new HashSet<>());
    }

    public int shardsUnassigned() {
        return shardCopiesToAssign.size();
    }

    public int getUnassignedShard() {
        return shardCopiesToAssign.remove();
    }

    public int assignShard(ActorRef slave) {
        int shard = getUnassignedShard();
        if (shardToSlaves.get(shard).size() > 0) {
            // requires copying
            assign(slave, shard, false);
        } else {
            assign(slave, shard, true);
        }
        return shard;
    }
}
