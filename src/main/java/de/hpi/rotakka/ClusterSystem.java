package de.hpi.rotakka;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.hpi.rotakka.actors.cluster.ClusterListener;
import de.hpi.rotakka.actors.cluster.MetricsListener;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

import java.util.concurrent.TimeUnit;

abstract class ClusterSystem {

	private Config createConfiguration() {
		// Create the Config with fallback to the application config
		return ConfigFactory.parseString(
				"akka.remote.netty.tcp.hostname = \"" + host + "\"\n" +
				"akka.remote.netty.tcp.port = " + port + "\n" + 
				"akka.remote.artery.canonical.hostname = \"" + host + "\"\n" +
				"akka.remote.artery.canonical.port = " + port + "\n" +
						"akka.cluster.roles = [" + this.getRoleName() + "]\n" +
						"akka.cluster.seed-nodes = [\"akka://" + MainApp.ACTOR_SYSTEM_NAME + "@" + masterhost + ":" + masterport + "\"]")
				.withFallback(ConfigFactory.load("rotakka"));
	}

	private ActorSystem createSystem() {
		
		// Create the ActorSystem
		final ActorSystem system = ActorSystem.create(MainApp.ACTOR_SYSTEM_NAME, this.config);
		
		// Register a callback that ends the program when the ActorSystem terminates
		system.registerOnTermination(() -> System.exit(0));
		
		// Register a callback that terminates the ActorSystem when it is detached from the cluster
		Cluster.get(system).registerOnMemberRemoved(() -> {
			system.terminate();

			new Thread(() -> {
				try {
					Await.ready(system.whenTerminated(), Duration.create(10, TimeUnit.SECONDS));
				} catch (Exception e) {
					System.exit(-1);
				}
			}).start();
		});
		
		return system;
	}

	static final String ROLE = "system";

	abstract String getRoleName();

	private final String host;
	private final int port;
	private final String masterhost;
	private final int masterport;


	private final Config config;
	private final ActorSystem system;
	private final ClusterSingletonManagerSettings clusterSingletonManagerSettings;

	ClusterSystem(String host, int port) {
		this.host = host;
		this.port = port;
		this.masterhost = host;
		this.masterport = port;
		this.config = createConfiguration();
		this.system = createSystem();
		this.clusterSingletonManagerSettings = ClusterSingletonManagerSettings.create(system);
	}

	ClusterSystem(String host, int port, String masterhost, int masterport) {
		this.host = host;
		this.port = port;
		this.masterhost = masterhost;
		this.masterport = masterport;
		this.config = createConfiguration();
		this.system = createSystem();
		this.clusterSingletonManagerSettings = ClusterSingletonManagerSettings.create(system);
	}

	void start() {
		Cluster.get(system).registerOnMemberUp(() -> {
			addSingletons();
			startActors();
		});
	}

	private void addSingletons() {
		system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
		system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
	}

	abstract void startActors();
}
