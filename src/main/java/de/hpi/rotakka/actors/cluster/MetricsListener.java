package de.hpi.rotakka.actors.cluster;

import akka.actor.Props;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.metrics.ClusterMetricsChanged;
import akka.cluster.metrics.ClusterMetricsExtension;
import akka.cluster.metrics.NodeMetrics;
import akka.cluster.metrics.StandardMetrics;
import akka.cluster.metrics.StandardMetrics.Cpu;
import akka.cluster.metrics.StandardMetrics.HeapMemory;
import de.hpi.rotakka.actors.AbstractClusterActor;

public class MetricsListener extends AbstractClusterActor {

	public static String DEFAULT_NAME = "metricsListener";
	public static Props props() {
		return Props.create(MetricsListener.class);
	}

	private final ClusterMetricsExtension extension = ClusterMetricsExtension.get(getContext().system());
	
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
			.build();
	}
	
	private void logMetrics(ClusterMetricsChanged clusterMetrics) {
		for (NodeMetrics nodeMetrics : clusterMetrics.getNodeMetrics()) {
			if (nodeMetrics.address().equals(this.cluster.selfAddress())) {
				logHeap(nodeMetrics);
				logCpu(nodeMetrics);
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
			this.log.debug("Load: {} ({} processors)", cpu.systemLoadAverage().get(), cpu.processors());
		}
	}
}
