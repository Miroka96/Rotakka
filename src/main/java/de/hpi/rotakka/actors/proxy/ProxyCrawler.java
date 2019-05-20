package de.hpi.rotakka.actors.proxy;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class ProxyCrawler extends AbstractActor {

    public static final String DEFAULT_NAME = "proxyCrawler";
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props() {
        return Props.create(ProxyCrawler.class);
    }

    @Data
    @AllArgsConstructor
    public static final class ExtractProxies implements Serializable {
        public static final long serialVersionUID = 1L;
        String url;
    }

    @Data
    @AllArgsConstructor
    public static final class GetProxies implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Override
    public void preStart() {}

    @Override
    public void postStop() {}

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(
                ExtractProxies.class,
                r -> {
                    log.info("Recieved Message to Extract proxies");
                })
                .build();


    }
}
