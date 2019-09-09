package de.hpi.rotakka;

import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.cluster.Cluster;
import akka.cluster.ddata.DistributedData;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.hpi.rotakka.actors.cluster.ClusterListener;
import de.hpi.rotakka.actors.cluster.MetricsListener;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster;
import de.hpi.rotakka.actors.data.graph.GraphStoreSlave;
import de.hpi.rotakka.actors.proxy.checking.ProxyChecker;
import de.hpi.rotakka.actors.proxy.checking.ProxyCheckingScheduler;
import de.hpi.rotakka.actors.proxy.crawling.ProxyCrawler;
import de.hpi.rotakka.actors.proxy.crawling.ProxyCrawlingScheduler;
import de.hpi.rotakka.actors.twitter.TwitterCrawler;
import de.hpi.rotakka.actors.twitter.TwitterCrawlingScheduler;
import org.jetbrains.annotations.NotNull;
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

    @NotNull
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
                    Await.ready(system.whenTerminated(), Duration.create(2, TimeUnit.SECONDS));
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
    private final SettingsExtension settings;
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
        this.settings = Settings.SettingsProvider.get(system);
        this.clusterSingletonManagerSettings = ClusterSingletonManagerSettings.create(system);
        this.clusterSingletonProxySettings = ClusterSingletonProxySettings.create(system);
    }

    void start() {
        Cluster.get(system).registerOnMemberUp(() -> {
            addDefaultActors();
            customStart();
        });
    }

    private void addProxy(String singletonName, String proxyName) {
        String singletonManagerPath = "/user/" + singletonName;
        system.actorOf(
                ClusterSingletonProxy.props(
                        singletonManagerPath,
                        clusterSingletonProxySettings
                ), proxyName);
    }

    private void addDefaultActors() {
        ////////////// Singleton Managers /////////////////

        system.actorOf(
                ClusterSingletonManager.props(
                        ProxyCheckingScheduler.props(),
                        PoisonPill.getInstance(),
                        clusterSingletonManagerSettings),
                ProxyCheckingScheduler.DEFAULT_NAME);
        addProxy(ProxyCheckingScheduler.DEFAULT_NAME, ProxyCheckingScheduler.PROXY_NAME);

        system.actorOf(
                ClusterSingletonManager.props(
                        ProxyCrawlingScheduler.props(),
                        PoisonPill.getInstance(),
                        clusterSingletonManagerSettings),
                ProxyCrawlingScheduler.DEFAULT_NAME);
        addProxy(ProxyCrawlingScheduler.DEFAULT_NAME, ProxyCrawlingScheduler.PROXY_NAME);

        system.actorOf(
                ClusterSingletonManager.props(
                        TwitterCrawlingScheduler.props(),
                        PoisonPill.getInstance(),
                        clusterSingletonManagerSettings),
                TwitterCrawlingScheduler.DEFAULT_NAME);
        addProxy(TwitterCrawlingScheduler.DEFAULT_NAME, TwitterCrawlingScheduler.PROXY_NAME);

        system.actorOf(
                ClusterSingletonManager.props(
                        GraphStoreMaster.props(settings.graphStoreShardCount, settings.graphStoreDuplicationLevel),
                        PoisonPill.getInstance(),
                        clusterSingletonManagerSettings),
                GraphStoreMaster.DEFAULT_NAME);
        addProxy(GraphStoreMaster.DEFAULT_NAME, GraphStoreMaster.PROXY_NAME);

        // the replicator is automatically started by the DistributedData extension
        DistributedData.apply(system);

        //////////////// worker actors ///////////////////////////////////////
        if (settings.createClusterListener) {
            system.actorOf(ClusterListener.props(), ClusterListener.DEFAULT_NAME);
        }

        if (settings.createMetricsListener) {
            system.actorOf(MetricsListener.props(), MetricsListener.DEFAULT_NAME);
        }

        for (int i = 0; i < settings.proxyCheckingSlaveCount; i++) {
            system.actorOf(ProxyChecker.props(), ProxyChecker.DEFAULT_NAME + "-" + i);
        }

        for (int i = 0; i < settings.proxyCrawlingSlaveCount; i++) {
            system.actorOf(ProxyCrawler.props(), ProxyCrawler.DEFAULT_NAME + "-" + i);
        }

        for (int i = 0; i < settings.twitterCrawlingSlaveCount; i++) {
            system.actorOf(TwitterCrawler.props(), TwitterCrawler.DEFAULT_NAME + "-" + i);
        }

        for (int i = 0; i < settings.graphStoreSlaveCount; i++) {
            system.actorOf(GraphStoreSlave.props(), GraphStoreSlave.DEFAULT_NAME + "-" + i);
        }
    }

    abstract void customStart();
}
