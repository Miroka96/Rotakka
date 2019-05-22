package de.hpi.rotakka.actors.proxy;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.rotakka.actors.proxy.utility.Crawler;
import de.hpi.rotakka.actors.proxy.websites.CrawlerFreeProxyCZ;
import de.hpi.rotakka.actors.proxy.utility.Proxy;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.List;

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

    // WIP; Not used ATM
    private void crawlURL(ExtractProxies message) {
        String url = message.getUrl();

        Crawler crawler = null;
        if (url.startsWith("http://free-proxy.cz/")) {
            crawler = new CrawlerFreeProxyCZ();
        }
        else {
            log.error("Could not find a matching crawler to that URL");
        }

        List<Proxy> proxies = crawler.extract(url);
    }
}
