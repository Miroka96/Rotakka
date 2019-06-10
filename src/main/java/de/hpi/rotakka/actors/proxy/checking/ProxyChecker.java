package de.hpi.rotakka.actors.proxy.checking;

import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.proxy.ProxyWrapper;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;

public class ProxyChecker extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "proxyChecker";

    public static Props props() {
        return Props.create(ProxyChecker.class);
    }

    @Data
    @AllArgsConstructor
    public static final class CheckProxyAddress implements Serializable {
        public static final long serialVersionUID = 1L;
        ProxyWrapper proxy;
    }

    @Override
    public void preStart() {
        ProxyCheckingScheduler.getSingleton(context()).tell(new Messages.RegisterMe(), getSelf());
    }

    @Override
    public void postStop() {}

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(
                        CheckProxyAddress.class, this::handleCheckProxyAddress)
                .build();
    }

    /**
     * This method will handle the CheckProxyAddress message
     */
    private void handleCheckProxyAddress(CheckProxyAddress message) {
        ProxyWrapper proxy = message.proxy;
        if(isReachable(proxy)) {
            long respTime = averageResponseTime(proxy);
            if(respTime >= 0) {
                proxy.setAverageResponseTime(respTime);
            }
            //ToDo: Sent a message back to the Syncher/DataStore with the checked proxy
            //      It is debatable whether we should also save proxies which have failed the check
        }
    }

    /**
     * This method will determine whether a proxy is reachable (i.e. ICMP check)
     */
    private Boolean isReachable(ProxyWrapper proxy) {
        URLConnection connection = null;
        try {
            InetAddress address = InetAddress.getByName(proxy.getIp());
            boolean reachable = address.isReachable(10000);
            if(reachable) {
                connection = new URL("http://www.google.com").openConnection(proxy.getProxyObject());
                connection.setConnectTimeout(10000);
                connection.connect();
                Object content = connection.getContent();
                return true;
            }
        } catch(IOException e) {
            return false;
        } finally {
            if (connection != null) {
                try {
                    connection.getInputStream().close();
                } catch (Exception ignored) {
                }
            }
        }
        return false;
    }

    /**
     * This method will calculate the average response time of a proxy over 5 samples to Google
     */
    private long averageResponseTime(ProxyWrapper proxy) {
        try {
            long timeDifference = 0;
            int checks = 5;
            for (int i = 0; i < checks; i++) {
                Long startTime = System.currentTimeMillis();
                URLConnection connection = new URL("http://www.google.com").openConnection(proxy.getProxyObject());
                connection.setConnectTimeout(10000);
                connection.connect();
                Object content = connection.getContent();
                Long endTime = System.currentTimeMillis();
                timeDifference += (endTime-startTime);
            }
            return timeDifference / checks;
        }
        catch(IOException e) {
            return -1;
        }
    }
}
