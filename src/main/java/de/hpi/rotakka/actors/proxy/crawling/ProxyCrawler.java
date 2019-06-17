package de.hpi.rotakka.actors.proxy.crawling;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.rotakka.actors.proxy.ProxyWrapper;
import de.hpi.rotakka.actors.proxy.checking.ProxyCheckingScheduler;
import de.hpi.rotakka.actors.proxy.crawling.websites.CrawlerFreeProxyCZ;
import de.hpi.rotakka.actors.proxy.crawling.websites.CrawlerUsProxy;
import de.hpi.rotakka.actors.utils.Crawler;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class ExtractProxies implements Serializable {
        // Take the specific crawler and extract all proxies from that website
        public static final long serialVersionUID = 1L;
        String crawlerName;
    }

    @Override
    public void preStart() {
        ProxyCrawlingScheduler.getSingleton(context()).tell(new Messages.RegisterMe(), getSelf());
    }

    @Override
    public void postStop() {}

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(
                ExtractProxies.class, this::extract)
                .build();
    }

    private void extract(ExtractProxies task) {
        extract(task.crawlerName);
    }

    // ToDo: We could think about micro-batching this instead of scraping all and then adding it
    /**
     * This method will determine which crawler should be started by performing a string check and then
     * calling the extract() of the specific crawler
     */
    private void extract(String crawlerName) {
        Crawler crawler;
        log.info("Tasked to Crawl "+crawlerName);
        if(crawlerName.equals("CrawlerFreeProxyCZ")) {
            crawler = new CrawlerFreeProxyCZ();
        }
        else if(crawlerName.equals("CrawlerUsProxy")) {
            crawler = new CrawlerUsProxy();
        }
        else {
            log.error("FATAL: RotakkarProxy Crawler Class not found");
            return;
        }
        List<ProxyWrapper> proxies = crawler.extract();
        log.info("Extracted "+proxies.size()+" proxies");

        // Single Proxy Sending
        for(ProxyWrapper proxy : proxies) {
            ProxyCheckingScheduler.getSingleton(context()).tell(proxy, getSelf());
        }
        ProxyCrawlingScheduler.getSingleton(context()).tell(new ProxyCrawlingScheduler.FinishedScraping(), getSelf());
        log.info("Finished sending messages for "+proxies.size()+" proxies");

    }
}
