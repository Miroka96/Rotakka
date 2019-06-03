package de.hpi.rotakka.actors.proxy.crawling;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.rotakka.actors.proxy.ProxyWrapper;
import de.hpi.rotakka.actors.proxy.crawling.websites.CrawlerFreeProxyCZ;
import de.hpi.rotakka.actors.utils.Crawler;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ProxyCrawler extends AbstractActor {

    public static final String DEFAULT_NAME = "proxyCrawler";
    private final LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
    public static Props props() {
        return Props.create(ProxyCrawler.class);
    }

    private List<ProxyWrapper> proxyStore = new ArrayList<>();

    @Data
    @AllArgsConstructor
    public static final class ExtractProxies implements Serializable {
        // Take the specific crawler and extract all proxies from that website
        public static final long serialVersionUID = 1L;
        String crawlerName;
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
                ExtractProxies.class, this::crawl)
                .build();
    }

    private void crawl(ExtractProxies task) {
        crawl(task.crawlerName);
    }

    // ToDo: We could think about micro-batching this instead of scraping all and then adding it
    /**
     * This method will determine which crawler should be started by performing a string check and then
     * calling the extract() of the specific crawler
     */
    private void crawl(String crawlerName) {
        Crawler crawler;
        if(crawlerName.equals("CrawlerFreeProxyCZ")) {
            crawler = new CrawlerFreeProxyCZ();
        }
        else {
            log.error("FATAL: RotakkarProxy Crawler Class not found");
            return;
        }
        List<ProxyWrapper> proxies = crawler.extract();
        this.proxyStore.addAll(proxies);
    }
}
