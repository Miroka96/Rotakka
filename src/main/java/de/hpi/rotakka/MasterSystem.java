package de.hpi.rotakka;


import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import com.typesafe.config.Config;
import de.hpi.rotakka.actors.Initiator;

import java.util.EventListener;

class MasterSystem extends ClusterSystem {

	static final String MASTER_ROLE = "master";

	static void start(String actorSystemName, int workers, String host, int port) {

		final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port, host, port);
		
		final ActorSystem system = createSystem(actorSystemName, config);

		Cluster.get(system).registerOnMemberUp(() -> {
			addSystemSingletons(system);
			system.actorOf(Initiator.props(), Initiator.DEFAULT_NAME);


			//	int maxInstancesPerNode = workers; // TODO: Every node gets the same number of workers, so it cannot be a parameter for the slave nodes
			//	Set<String> useRoles = new HashSet<>(Arrays.asList("master", "slave"));
			//	ActorRef router = system.actorOf(
			//		new ClusterRouterPool(
			//			new AdaptiveLoadBalancingPool(SystemLoadAverageMetricsSelector.getInstance(), 0),
			//			new ClusterRouterPoolSettings(10000, workers, true, new HashSet<>(Arrays.asList("master", "slave"))))
			//		.props(Props.create(Worker.class)), "router");

			system.actorSelection("/user/" + Initiator.DEFAULT_NAME).tell(new Initiator.RunConfiguration(), ActorRef.noSender());
		});
	}
}
