package de.hpi.rotakka.actors.twitter;

import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.data.graph.GraphStoreMaster;
import de.hpi.rotakka.actors.proxy.CheckedProxy;
import de.hpi.rotakka.actors.utils.Messages;
import de.hpi.rotakka.actors.utils.Tweet;
import de.hpi.rotakka.actors.utils.WebDriverFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class TwitterCrawler extends AbstractLoggingActor {

    public final static String DEFAULT_NAME = "twitterCrawler";
    private final static int PAGE_LOAD_WAIT = 2000;
    private final static int PAGE_AJAX_WAIT = 2000;
    private int proxyChangeCounter = Integer.MAX_VALUE;

    public static Props props() {
        return Props.create(TwitterCrawler.class);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class CrawlURL implements Serializable {
        public static final long serialVersionUID = 1L;
        String url;
        CheckedProxy proxy;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CrawlURL.class, this::handleCrawlURL)
                .build();
    }

    private WebDriver webDriver;
    private List<Tweet> extractedTweets = new ArrayList<>();

    private void handleCrawlURL(@NotNull CrawlURL message) {
        try {
            crawl(message.getUrl(), message.getProxy());
        } catch (WebDriverException e) {
            log.error("Website " + message.getUrl() + " could not be crawled: " + e.getMessage());
            return;
        }

        getSender().tell(new TwitterCrawlingScheduler.FinishedWork(), getSelf());
    }

    private void changeProxy(CheckedProxy proxy) {
        if (webDriver != null) {
            webDriver.close();
        }
        if (proxy != null) {
            webDriver = WebDriverFactory.createWebDriver(log, this.context(), proxy);
        } else {
            log.info("Starting WebDriver without a Proxy");
            webDriver = WebDriverFactory.createWebDriver(log, this.context());
        }
    }

    private void crawl(String url, CheckedProxy proxy) throws WebDriverException {
        log.info("Started working on:" + url);

        // ToDo: Fix non-working proxies
        if (proxyChangeCounter > 3) {
            log.info("Changing Proxy");
            changeProxy(proxy);
            proxyChangeCounter = 0;
        } else {
            proxyChangeCounter++;
        }

        webDriver.get(url);
        HashSet<String> newUsers = new HashSet<>();

        try {
            Thread.sleep(PAGE_LOAD_WAIT);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        long previousPageLength;
        while (true) {
            previousPageLength = Jsoup.parse(webDriver.getPageSource()).text().length();
            ((JavascriptExecutor) webDriver).executeScript("window.scrollTo(0, document.body.scrollHeight)");
            try {
                Thread.sleep(PAGE_AJAX_WAIT);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (previousPageLength == Jsoup.parse(webDriver.getPageSource()).text().length()) {
                log.info("Reached End of Page; Gathering Tweets");
                break;
            }
        }
        Document twPage = Jsoup.parse(webDriver.getPageSource());
        Elements tweets = twPage.select("ol[id=stream-items-id] li[data-item-type=tweet]");
        for (Element tweetHTML : tweets) {
            Element tweetDiv = tweetHTML.children().get(0);
            tweetDiv.children().select("div[class=content]");
            Tweet tweet = new Tweet(tweetDiv);
            newUsers.addAll(tweet.getReferenced_users());
            GraphStoreMaster.getSingleton(getContext()).tell(tweet.toVertex(), getSelf());
            //extractedTweets.add(tweet);
        }
        log.info("Scraped " + extractedTweets.size() + " tweets");
        log.info("Found " + newUsers.size() + " new users");
        // ToDo: Send the scraped tweets to the graph store
        // ToDo: Send the mentions to the TwitterCrawlingScheduler
        if (newUsers.size() > 0) {
            log.info("Found " + newUsers.size() + " new users");
            getSender().tell(new TwitterCrawlingScheduler.NewReference(newUsers), getSelf());
        }
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        TwitterCrawlingScheduler.getSingleton(context()).tell(new Messages.RegisterMe(), getSelf());
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        if (webDriver != null) {
            try {
                webDriver.close();
            } catch (WebDriverException e) {
                log.error("Web Driver could not be closed properly: " + e.getMessage());
            }
        }
    }

}
