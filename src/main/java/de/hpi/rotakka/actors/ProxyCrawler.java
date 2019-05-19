package de.hpi.rotakka.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.io.Serializable;

public class ProxyCrawler extends AbstractActor {

    public static final String DEFAULT_NAME = "proxyCrawler";
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props() {
        return Props.create(ProxyCrawler.class);
    }

    public static final class ExtractProxies implements Serializable {
        final String URL;

        public ExtractProxies(String URL) {
            this.URL = URL;
        }
    }

    public static final class GetProxies implements Serializable {}

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
