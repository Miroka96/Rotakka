package de.hpi.rotakka.actors.cluster;

import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.metrics.ClusterMetricsChanged;
import akka.cluster.metrics.ClusterMetricsExtension;
import akka.cluster.metrics.NodeMetrics;
import akka.cluster.metrics.StandardMetrics;
import akka.cluster.metrics.StandardMetrics.Cpu;
import akka.cluster.metrics.StandardMetrics.HeapMemory;
import de.hpi.rotakka.actors.AbstractClusterActor;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class MetricsListener extends AbstractClusterActor {

    public static String DEFAULT_NAME = "metricsListener";

    public static Props props() {
        return Props.create(MetricsListener.class);
    }

    private final ClusterMetricsExtension extension = ClusterMetricsExtension.get(system);
    private long scrapedTweets = 0;
    private long finishedUsers = 0;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class GraphStoreStatistic implements Serializable {
        public static final long serialVersionUID = 1L;
        public int assignedVertices = 0;
        public int updatedVertices = 0;
        public int assignedEdges = 0;
        public int updatedEdges = 0;

        public void add(@NotNull GraphStoreStatistic other) {
            this.assignedVertices += other.assignedVertices;
            this.updatedVertices += other.updatedVertices;
            this.assignedEdges += other.assignedEdges;
            this.updatedEdges += other.updatedEdges;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class GraphShardStatistic implements Serializable {
        public static final long serialVersionUID = 1L;
        public int shardNumber;
        public GraphStoreStatistic statistic;
    }

    private Map<Integer, GraphStoreStatistic> shardStatistics = new HashMap<>();
    private GraphStoreStatistic globalGraphStoreStatistic = new GraphStoreStatistic();

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class ScrapedTweetCount implements Serializable {
        public static final long serialVersionUID = 1L;
        public int tweetCount;
    }

    @NoArgsConstructor
    public static final class FinishedUser implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    public static ActorSelection getRef(@NotNull akka.actor.ActorContext context) {
        return context.actorSelection("/user/" + DEFAULT_NAME);
    }

    @Override
    public void preStart() {
        this.extension.subscribe(self());
    }

    @Override
    public void postStop() {
        this.extension.unsubscribe(self());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClusterMetricsChanged.class, this::logMetrics)
                .match(CurrentClusterState.class, message -> {/*Ignore*/})
                .match(ScrapedTweetCount.class, this::handleScrapedTweetCount)
                .match(FinishedUser.class, msg -> finishedUsers++)
                .match(GraphShardStatistic.class, this::add)
                .build();
    }

    private void logMetrics(@NotNull ClusterMetricsChanged clusterMetrics) {
        for (NodeMetrics nodeMetrics : clusterMetrics.getNodeMetrics()) {
            if (nodeMetrics.address().equals(this.cluster.selfAddress())) {
                logHeap(nodeMetrics);
                logCpu(nodeMetrics);
                logTweetMetrics();
                logGraphStoreMetrics();
            }
        }
    }

    private void logHeap(NodeMetrics nodeMetrics) {
        HeapMemory heap = StandardMetrics.extractHeapMemory(nodeMetrics);
        if (heap != null) {
            this.log.debug("Used heap: {} MB", ((double) heap.used()) / 1024 / 1024);
        }
    }

    private void logCpu(NodeMetrics nodeMetrics) {
        Cpu cpu = StandardMetrics.extractCpu(nodeMetrics);
        if (cpu != null && cpu.systemLoadAverage().isDefined()) {
            log.debug("Load: {} ({} processors)", cpu.systemLoadAverage().get(), cpu.processors());
        }
    }

    private void logTweetMetrics() {
        log.info("Total Scraped Tweets: {}", scrapedTweets);
        log.info("Total Scraped Users: {}", finishedUsers);
    }

    private void handleScrapedTweetCount(@NotNull ScrapedTweetCount message) {
        if (Long.MAX_VALUE - scrapedTweets > message.tweetCount) {
            scrapedTweets += message.tweetCount;
        }
    }

    private void add(@NotNull GraphShardStatistic shardStatistic) {
        assert shardStatistic.statistic != null : "Received GraphShardStatistic with empty statistic field";
        GraphStoreStatistic graphStoreStatistic = shardStatistics.computeIfAbsent(shardStatistic.shardNumber, ignore -> new GraphStoreStatistic());
        graphStoreStatistic.add(shardStatistic.statistic);
        globalGraphStoreStatistic.add(shardStatistic.statistic);
    }

    private void logGraphStoreMetrics() {
        log.info("Assigned Vertices Count: {}", globalGraphStoreStatistic.assignedVertices);
        log.info("Updated Vertices Count: {}", globalGraphStoreStatistic.updatedVertices);
        log.info("Assigned Edges Count: {}", globalGraphStoreStatistic.assignedEdges);
        log.info("Updated Edges Count: {}", globalGraphStoreStatistic.updatedEdges);

        shardStatistics.forEach((shardNumber, shardStatistics) -> {
            log.debug("Shard {}: Assigned Vertices Count: {}", shardNumber, shardStatistics.assignedVertices);
            log.debug("Shard {}: Updated Vertices Count: {}", shardNumber, shardStatistics.updatedVertices);
            log.debug("Shard {}: Assigned Edges Count: {}", shardNumber, shardStatistics.assignedEdges);
            log.debug("Shard {}: Updated Edges Count: {}", shardNumber, shardStatistics.updatedEdges);
        });
    }

}
