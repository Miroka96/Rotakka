package de.hpi.rotakka.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.io.Serializable;

public class ProxyChecker extends AbstractActor {

    public static final String DEFAULT_NAME = "proxyChecker";
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);

    public static Props props() {
        return Props.create(ProxyChecker.class);
    }

    public static final class CheckProxyAdress implements Serializable {
        final String IP;
        final int port;

        public CheckProxyAdress(String IP, int port) {
            this.IP = IP;
            this.port = port;
        }
    }

    @Override
    public void preStart() {}

    @Override
    public void postStop() {}

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(
                        CheckProxyAdress.class,
                        r -> {
                            log.info("Got Message to check Proxy");
                        })
                .build();
    }
}
