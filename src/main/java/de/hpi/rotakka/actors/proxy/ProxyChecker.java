package de.hpi.rotakka.actors.proxy;

import akka.actor.Props;
import de.hpi.rotakka.actors.LoggingActor;
import de.hpi.rotakka.actors.proxy.utility.RotakkaProxy;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.io.Serializable;
import java.net.*;

public class ProxyChecker extends LoggingActor {

    public static final String DEFAULT_NAME = "proxyChecker";

    public static Props props() {
        return Props.create(ProxyChecker.class);
    }

    @Data
    @AllArgsConstructor
    public static final class CheckProxyAddress implements Serializable {
        public static final long serialVersionUID = 1L;
        RotakkaProxy proxy;
    }

    @Override
    public void preStart() {}

    @Override
    public void postStop() {}

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(
                        CheckProxyAddress.class, r -> this.isReachable(r.getProxy()))
                .build();
    }

    private boolean isReachable(RotakkaProxy proxy) {
        try {
            InetAddress address = InetAddress.getByName(proxy.getIp());
            boolean reachable = address.isReachable(10000);
            if(reachable) {
                URLConnection connection = new URL("http://www.google.com").openConnection(proxy.getProxyObject());
                connection.setConnectTimeout(10000);
                connection.connect();
                Object content = connection.getContent();
                return true;
            }
        }
        catch(IOException e) {
            log.error("Proxy is not reachable");
            log.error(e.getMessage());
        }
        return false;
    }
}
