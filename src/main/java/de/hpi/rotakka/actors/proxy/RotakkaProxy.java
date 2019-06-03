package de.hpi.rotakka.actors.proxy;

import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.utils.ProxyConfig;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class RotakkaProxy extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "rotakkaProxy";

    public static Props props() {
        return Props.create(RotakkaProxy.class);
    }

    @Data
    @AllArgsConstructor
    public static final class Proxies implements Serializable {
        public static final long serialVersionUID = 1L;
        ProxyConfig proxies[];
    }

    @Data
    @AllArgsConstructor
    public static final class URL implements Serializable {
        public static final long serialVersionUID = 1L;
        String url;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Proxies.class, this::addProxies)
                .match(URL.class, this::getUrl)
                .build();
    }

    private void addProxies(Proxies newProxies) {
        addProxies(newProxies.proxies);
    }

    private void addProxies(ProxyConfig proxies[]) {

    }

    private void getUrl(URL url) {
        getUrl(url.url);
    }

    private void getUrl(String url) {

    }

}
