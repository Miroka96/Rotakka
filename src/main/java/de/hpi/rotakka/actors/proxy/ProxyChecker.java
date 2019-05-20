package de.hpi.rotakka.actors.proxy;

import akka.actor.Props;
import de.hpi.rotakka.actors.LoggingActor;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class ProxyChecker extends LoggingActor {

    public static final String DEFAULT_NAME = "proxyChecker";

    public static Props props() {
        return Props.create(ProxyChecker.class);
    }

    @Data
    @AllArgsConstructor
    public static final class CheckProxyAddress implements Serializable {
        public static final long serialVersionUID = 1L;
        String ip;
        int port;
    }

    @Override
    public void preStart() {}

    @Override
    public void postStop() {}

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(
                        CheckProxyAddress.class,
                        r -> {
                            log.info("Got Message to check Proxy");
                        })
                .build();
    }
}
