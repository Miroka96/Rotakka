package de.hpi.rotakka.actors.proxy.crawling;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractReplicationActor;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;


public class ProxyCrawlingScheduler extends AbstractReplicationActor {

    public static final String DEFAULT_NAME = "proxyCrawlingScheduler";
    public static final String PROXY_NAME = DEFAULT_NAME + "Proxy";
    private ArrayList<ActorRef> proxyCrawlers = new ArrayList<>();
    private ArrayList<ActorRef> availableWorkers = new ArrayList<>();
    private final ArrayList<String> proxySites = new ArrayList<>(Arrays.asList("CrawlerUsProxy", "CrawlerFreeProxyCZ"));
    private ArrayList<String> tempProxySites = new ArrayList<>(Arrays.asList("CrawlerUsProxy", "CrawlerFreeProxyCZ"));

    public static ActorSelection getSingleton(@NotNull akka.actor.ActorContext context) {
        return context.actorSelection("/user/" + PROXY_NAME);
    }

    public static Props props() {
        return Props.create(ProxyCrawlingScheduler.class);
    }

    @Override
    public void preStart() {
        system.scheduler().schedule(Duration.ofMinutes(30), Duration.ofMinutes(30), getSelf(), new RecrawlProxySite(), system.dispatcher(), getSelf());
    }

    @Data
    @AllArgsConstructor
    public static final class IntegrateNewProxies implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Data
    @AllArgsConstructor
    public static final class FinishedScraping implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Data
    @AllArgsConstructor
    public static final class RecrawlProxySite implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.RegisterMe.class, this::add)
                .match(FinishedScraping.class, this::handleFinishedScraping)
                .match(IntegrateNewProxies.class, this::handleIntegrateNewProxies)
                .match(RecrawlProxySite.class, this::handleRecrawlProxySite)
                .build();
    }

    private void add(Messages.RegisterMe msg) {
        proxyCrawlers.add(getSender());
        handleFreeWorker();
    }

    private void handleFinishedScraping(FinishedScraping msg) {
        availableWorkers.add(getSender());
        handleFreeWorker();
    }

    // This functionality will be handled by the ProxyCheckingScheduler
    private void handleIntegrateNewProxies(IntegrateNewProxies msg) {
        // ToDo
    }

    private void handleFreeWorker() {
        if(tempProxySites.size() > 0) {
            String site = tempProxySites.get(0);
            tempProxySites.remove(site);
            sender().tell(new ProxyCrawler.ExtractProxies(site), getSelf());
        }
        else {
            availableWorkers.add(getSender());
        }
    }

    private void assignWork() {
        for(ActorRef workerRef : availableWorkers) {
            String site = tempProxySites.get(0);
            tempProxySites.remove(site);
            workerRef.tell(new ProxyCrawler.ExtractProxies(site), getSelf());
        }
    }

    private void handleRecrawlProxySite(RecrawlProxySite msg) {
        for(String proxySite : proxySites) {
            tempProxySites.add(proxySite);
        }
        assignWork();
    }

}
