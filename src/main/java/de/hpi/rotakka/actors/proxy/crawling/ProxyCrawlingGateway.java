package de.hpi.rotakka.actors.proxy.crawling;

import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class ProxyCrawlingGateway extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "proxySyncher";

    public static Props props() {
        return Props.create(ProxyCrawlingGateway.class);
    }

    @Data
    @AllArgsConstructor
    public static final class GetProxy implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Data
    @AllArgsConstructor
    public static final class IntegrateNewProxies implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Data
    @AllArgsConstructor
    public static final class IntegrateCheckedProxies implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Override
    public Receive createReceive() {
        return null;
    }

}
