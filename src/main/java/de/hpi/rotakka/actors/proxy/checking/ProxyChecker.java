package de.hpi.rotakka.actors.proxy.checking;

import akka.actor.ActorIdentity;
import akka.actor.Identify;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.proxy.CheckedProxy;
import de.hpi.rotakka.actors.proxy.ProxyWrapper;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.List;

public class ProxyChecker extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "proxyChecker";

    public static Props props() {
        return Props.create(ProxyChecker.class);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static final class CheckProxies implements Serializable {
        public static final long serialVersionUID = 1L;
        public List<ProxyWrapper> proxyList;
    }

    @Override
    public void preStart() {
        ProxyCheckingScheduler.getSingleton(context()).tell(new Messages.RegisterMe(), getSelf());
        ProxyCheckingScheduler.getSingleton(getContext()).tell(new Identify(2019), getSelf());
    }

    @Override
    public void postStop() {
        ProxyCheckingScheduler.getSingleton(context()).tell(new Messages.UnregisterMe(), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CheckProxies.class, this::handleCheckProxyAddress)
                .match(ActorIdentity.class, this::handleActorIdentity)
                .match(Terminated.class, this::handleDeadScheduler)
                .build();
    }

    private void handleActorIdentity(@NotNull ActorIdentity message) {
        // Watch the TwitterCrawlingScheduler to be notified of its death
        getContext().watch(message.getRef());
    }

    private void handleDeadScheduler(Terminated message) {
        ProxyCheckingScheduler.getSingleton(getContext()).tell(new Messages.RegisterMe(), getSelf());
        ProxyCheckingScheduler.getSingleton(getContext()).tell(new Identify(2019), getSelf());
    }

    /**
     * This method will handle the CheckProxyAddress message
     */
    private void handleCheckProxyAddress(@NotNull CheckProxies msg) {
        List<ProxyWrapper> proxyList = msg.getProxyList();
        for(ProxyWrapper proxy : proxyList) {
            if (isReachable(proxy)) {
                long respTime = averageResponseTime(proxy);
                if (respTime >= 0 && respTime < settings.maxiumResponseTime) {
                    proxy.setAverageResponseTime(respTime);
                    log.info("Proxy " + proxy.getIp() + " is working with ~" + proxy.getAverageResponseTime() + " ms");
                    CheckedProxy checkedProxy = new CheckedProxy(proxy);
                    ProxyCheckingScheduler.getSingleton(getContext()).tell(new ProxyCheckingScheduler.IntegrateCheckedProxy(checkedProxy), getSelf());
                }
            } else {
                log.info("Proxy " + proxy.getIp() + " is disabled");
                ProxyCheckingScheduler.getSingleton(getContext()).tell(new ProxyCheckingScheduler.GetWork(), getSelf());
            }
        }
    }

    /**
     * This method will determine whether a proxy is reachable (i.e. ICMP check)
     */
    @NotNull
    private Boolean isReachable(@NotNull ProxyWrapper proxy) {
        URLConnection connection = null;
        try {
            InetAddress address = InetAddress.getByName(proxy.getIp());
            boolean reachable = address.isReachable(10000);
            if(reachable) {
                connection = new URL("https://twitter.com/").openConnection(proxy.getProxyObject());
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
