package de.hpi.rotakka.actors.proxy.crawling;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractReplicationActor;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;

public class ProxyCrawlingScheduler extends AbstractReplicationActor {

    public static final String DEFAULT_NAME = "proxyCrawlingScheduler";
    public static final String PROXY_NAME = DEFAULT_NAME + "Proxy";
    private ArrayList<ActorRef> proxyCrawlers = new ArrayList<>();
    private ArrayList<ActorRef> availableWorkers = new ArrayList<>();

    public static ActorSelection getSingleton(akka.actor.ActorContext context) {
        return context.actorSelection("/user/" + PROXY_NAME);
    }

    public static Props props() {
        return Props.create(ProxyCrawlingScheduler.class);
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

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.RegisterMe.class, this::add)
                .match(FinishedScraping.class, this::handleFinishedScraping)
                .match(IntegrateNewProxies.class, this::handleIntegrateNewProxies)
                .build();
    }

    private void add(Messages.RegisterMe msg) {
        proxyCrawlers.add(getSender());
        availableWorkers.add(getSender());
    }

    private void handleFinishedScraping(FinishedScraping message) {
        availableWorkers.add(getSender());
    }

    // This functionality will be handled by the ProxyCheckingScheduler
    private void handleIntegrateNewProxies(IntegrateNewProxies message) {
        // ToDo
    }

}
