package de.hpi.rotakka.actors.proxy.crawling;

import akka.actor.*;
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
import org.jetbrains.annotations.NotNull;
import org.openqa.selenium.WebDriverException;

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
        ProxyCrawlingScheduler.getSingleton(getContext()).tell(new Messages.RegisterMe(), getSelf());
        ProxyCrawlingScheduler.getSingleton(getContext()).tell(new Identify(42), getSelf());
    }

    @Override
    public void postStop() {
        ProxyCrawlingScheduler.getSingleton(context()).tell(new Messages.UnregisterMe(), getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ExtractProxies.class, this::extract)
                .match(ActorIdentity.class, this::handleActorIdentity)
                .match(Terminated.class, this::handleDeadScheduler)
                .build();
    }

    private void extract(@NotNull ExtractProxies task) {
        extract(task.crawlerName);
    }

    private void handleActorIdentity(ActorIdentity message) {
        // Watch the TwitterCrawlingScheduler to be notified of its death
        getContext().watch(message.getRef());
    }

    private void handleDeadScheduler(Terminated message) {
        ProxyCrawlingScheduler.getSingleton(getContext()).tell(new Messages.RegisterMe(), getSelf());
        ProxyCrawlingScheduler.getSingleton(getContext()).tell(new Identify(42), getSelf());
    }

    // ToDo: We could think about micro-batching this instead of scraping all and then adding it
    /**
     * This method will determine which crawler should be started by performing a string check and then
     * calling the extract() of the specific crawler
     */
    private void extract(@NotNull String crawlerName) {
        Crawler crawler;
        log.info("Tasked to Crawl "+crawlerName);
        if(crawlerName.equals("CrawlerFreeProxyCZ")) {
            crawler = new CrawlerFreeProxyCZ(log);
        }
        else if(crawlerName.equals("CrawlerUsProxy")) {
            crawler = new CrawlerUsProxy(log);
        }
        else {
            log.error("FATAL: Rotakka Proxy Crawler Class not found");
            return;
        }
        List<ProxyWrapper> proxies = null;
        try {
            proxies = crawler.extract();
        } catch (WebDriverException e) {
            log.error("Could not extract proxies from " + crawlerName + ": " + e.getMessage());
            return;
        }

        log.info("Extracted "+proxies.size()+" proxies");

        // Single Proxy Sending
        for(ProxyWrapper proxy : proxies) {
            if(proxy != null) {
                ProxyCheckingScheduler.getSingleton(context()).tell(proxy, getSelf());
            }
            else {
                log.error("Proxy was null");
            }
        }
        ProxyCrawlingScheduler.getSingleton(context()).tell(new ProxyCrawlingScheduler.FinishedScraping(), getSelf());
        log.info("Finished sending messages for "+proxies.size()+" proxies");

    }
}
