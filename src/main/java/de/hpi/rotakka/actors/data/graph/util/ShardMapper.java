package de.hpi.rotakka.actors.data.graph.util;

import akka.actor.ActorContext;
import akka.actor.ActorRef;
import de.hpi.rotakka.actors.data.graph.GraphStoreBuffer;
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
    private HashMap<ActorRef, HashSet<Integer>> slaveToShards = new HashMap<>(5);

    public ShardMapper(int shardCount, int duplicationLevel, ActorContext context) {
        this.shardCount = shardCount;
        this.duplicationLevel = duplicationLevel;
        this.context = context;

        shardToSlaves = new ArrayList<>(shardCount);
        for (int shard = 0; shard < shardCount; shard++) {
            shardToSlaves.add(new HashMap<>(duplicationLevel));
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

    public void tellBuffer(int shardNumber, ActorRef slave, Object msg, ActorRef sender) {
        shardToSlaves.get(shardNumber).get(slave).tell(msg, sender);
    }

    public void tellShard(int shardNumber, Object msg, ActorRef sender) {
        shardToSlaves.get(shardNumber).forEach((slave, buffer) -> buffer.tell(msg, sender));
    }


    public void assign(ActorRef slave, int shard, boolean ready) {
        ActorRef buffer;
        if (ready) {
            buffer = context.actorOf(GraphStoreBuffer.props(shard, slave));
        } else {
            buffer = context.actorOf(GraphStoreBuffer.props(shard));
        }
        slaveToShards.putIfAbsent(slave, new HashSet<>());
        slaveToShards.get(slave).add(shard);
        shardToSlaves.get(shard).put(slave, buffer);
    }

    public void unassign(ActorRef slave, int shard) {
        ActorRef buffer = shardToSlaves.get(shard).get(slave);
        slaveToShards.get(slave).remove(shard);
        shardToSlaves.get(shard).remove(slave);
        context.stop(buffer);
    }

    public void enableShard(int shardNumber, ActorRef slave, ActorRef sender) {
        tellBuffer(shardNumber, slave, new GraphStoreBuffer.StopBuffering(slave), sender);
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
