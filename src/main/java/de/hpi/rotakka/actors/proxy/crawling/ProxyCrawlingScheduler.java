package de.hpi.rotakka.actors.proxy.crawling;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.ddata.DistributedData;
import de.hpi.rotakka.actors.AbstractReplicationActor;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class ProxyCrawlingScheduler extends AbstractReplicationActor {

    public static final String DEFAULT_NAME = "proxyCrawlingScheduler";
    private final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();

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
                .match(FinishedScraping.class, this::handleFinishedScraping)
                .match(IntegrateNewProxies.class, this::handleIntegrateNewProxies)
                .build();
    }

    private void handleFinishedScraping(FinishedScraping message) {
        // ToDo
    }

    private void handleIntegrateNewProxies(IntegrateNewProxies message) {
        // ToDo
    }

}
