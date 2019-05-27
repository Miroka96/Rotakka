package de.hpi.rotakka;

import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.cluster.Cluster;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.hpi.rotakka.actors.cluster.ClusterListener;
import de.hpi.rotakka.actors.cluster.MetricsListener;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave;
import de.hpi.rotakka.actors.proxy.RotakkaProxy;
import de.hpi.rotakka.actors.proxy.checking.ProxyChecker;
import de.hpi.rotakka.actors.proxy.checking.ProxyCheckingGateway;
import de.hpi.rotakka.actors.proxy.checking.ProxyCheckingScheduler;
import de.hpi.rotakka.actors.proxy.crawling.ProxyCrawler;
import de.hpi.rotakka.actors.proxy.crawling.ProxyCrawlingGateway;
import de.hpi.rotakka.actors.proxy.crawling.ProxyCrawlingScheduler;
import de.hpi.rotakka.actors.twitter.TwitterCrawler;
import de.hpi.rotakka.actors.twitter.WebsiteCrawlingGateway;
import de.hpi.rotakka.actors.twitter.WebsiteCrawlingScheduler;
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
	protected final ActorSystem system;
	private final ClusterSingletonManagerSettings clusterSingletonManagerSettings;
	private final ClusterSingletonProxySettings clusterSingletonProxySettings;

	ClusterSystem(String host, int port) {
		this(host, port, host, port);
	}

	ClusterSystem(String host, int port, String masterhost, int masterport) {
		this.host = host;
		this.port = port;
		this.masterhost = masterhost;
		this.masterport = masterport;
		this.config = createConfiguration();
		this.system = createSystem();
		this.clusterSingletonManagerSettings = ClusterSingletonManagerSettings.create(system);
		this.clusterSingletonProxySettings = ClusterSingletonProxySettings.create(system);
	}

	void start() {
		Cluster.get(system).registerOnMemberUp(() -> {
			addDefaultActors();
			addCustomActors();
		});
	}

	private void addDefaultActors() {
		////////////// Singleton Managers /////////////////
		system.actorOf(
				ClusterSingletonManager.props(
						ProxyCheckingGateway.props(),
						PoisonPill.getInstance(),
						clusterSingletonManagerSettings),
				ProxyCheckingGateway.DEFAULT_NAME);

		system.actorOf(
				ClusterSingletonManager.props(
						ProxyCheckingScheduler.props(),
						PoisonPill.getInstance(),
						clusterSingletonManagerSettings),
				ProxyCheckingScheduler.DEFAULT_NAME);

		system.actorOf(
				ClusterSingletonManager.props(
						ProxyCrawlingGateway.props(),
						PoisonPill.getInstance(),
						clusterSingletonManagerSettings),
				ProxyCrawlingGateway.DEFAULT_NAME);

		system.actorOf(
				ClusterSingletonManager.props(
						ProxyCrawlingScheduler.props(),
						PoisonPill.getInstance(),
						clusterSingletonManagerSettings),
				ProxyCrawlingScheduler.DEFAULT_NAME);

		system.actorOf(
				ClusterSingletonManager.props(
						WebsiteCrawlingGateway.props(),
						PoisonPill.getInstance(),
						clusterSingletonManagerSettings),
				WebsiteCrawlingGateway.DEFAULT_NAME);

		system.actorOf(
				ClusterSingletonManager.props(
						WebsiteCrawlingScheduler.props(),
						PoisonPill.getInstance(),
						clusterSingletonManagerSettings),
				WebsiteCrawlingScheduler.DEFAULT_NAME);

		system.actorOf(
				ClusterSingletonManager.props(
						GraphStoreMaster.props(),
						PoisonPill.getInstance(),
						clusterSingletonManagerSettings),
				GraphStoreMaster.DEFAULT_NAME);

		//////////////// worker actors ///////////////////////////////////////

		system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
		system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
		system.actorOf(GraphStoreSlave.props(), GraphStoreSlave.DEFAULT_NAME);
		system.actorOf(ProxyChecker.props(), ProxyChecker.DEFAULT_NAME);
		system.actorOf(ProxyCrawler.props(), ProxyCrawler.DEFAULT_NAME);
		system.actorOf(RotakkaProxy.props(), RotakkaProxy.DEFAULT_NAME);
		system.actorOf(TwitterCrawler.props(), TwitterCrawler.DEFAULT_NAME);

	}

	abstract void addCustomActors();
}
