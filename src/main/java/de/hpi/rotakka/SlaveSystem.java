package de.hpi.rotakka;

class SlaveSystem extends ClusterSystem {

	static final String ROLE = "slave";

	SlaveSystem(String host, int port, String masterhost, int masterport) {
		super(host, port, masterhost, masterport);
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
