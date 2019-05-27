package de.hpi.rotakka;


class MasterSystem extends ClusterSystem {

	static final String ROLE = "master";

	MasterSystem(String host, int port) {
		super(host, port);
	}

	String getRoleName() {
		return ROLE;
	}


	void startActors() {

		//system.actorOf(Initiator.props(), Initiator.DEFAULT_NAME);


		//	int maxInstancesPerNode = workers; // TODO: Every node gets the same number of workers, so it cannot be a parameter for the slave nodes
		//	Set<String> useRoles = new HashSet<>(Arrays.asList("master", "slave"));
		//	ActorRef router = system.actorOf(
		//		new ClusterRouterPool(
		//			new AdaptiveLoadBalancingPool(SystemLoadAverageMetricsSelector.getInstance(), 0),
		//			new ClusterRouterPoolSettings(10000, workers, true, new HashSet<>(Arrays.asList("master", "slave"))))
		//		.props(Props.create(Worker.class)), "router");

		//system.actorSelection("/user/" + Initiator.DEFAULT_NAME).tell(new Initiator.RunConfiguration(), ActorRef.noSender());
	}
}
