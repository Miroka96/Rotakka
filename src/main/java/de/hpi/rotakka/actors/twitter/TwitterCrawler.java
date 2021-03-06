package de.hpi.rotakka.actors.twitter;

import akka.actor.ActorIdentity;
import akka.actor.Identify;
import akka.actor.Props;
import akka.actor.Terminated;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.cluster.MetricsListener;
import de.hpi.rotakka.actors.graph.GraphStoreMaster;
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
import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import java.io.Serializable;
import java.util.HashSet;
import java.util.NoSuchElementException;

public class TwitterCrawler extends AbstractLoggingActor {

    public final static String DEFAULT_NAME = "twitterCrawler";
    private final int dynamicContentWait = settings.dynamicContentWait;
    private int proxyChangeCounter = Integer.MAX_VALUE;
    private final int REQUESTS_PER_PROXY = settings.requestPerProxy;

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
                .match(ActorIdentity.class, this::handleActorIdentity)
                .match(Terminated.class, this::handleDeadScheduler)
                .build();
    }

    private WebDriver webDriver;

    private void handleCrawlURL(@NotNull CrawlURL message) {
        try {
            crawl(message.getUrl(), message.getProxy());
        } catch (WebDriverException e) {
            log.error("Website " + message.getUrl() + " could not be crawled: " + e.getMessage());
        }
        getSender().tell(new TwitterCrawlingScheduler.FinishedWork(), getSelf());
    }

    private void handleDeadScheduler(Terminated message) {
        TwitterCrawlingScheduler.getSingleton(getContext()).tell(new Messages.RegisterMe(), getSelf());
        TwitterCrawlingScheduler.getSingleton(getContext()).tell(new Identify(1337), getSelf());
    }

    private void changeProxy(CheckedProxy proxy) {
        if (webDriver != null) {
            webDriver.close();
        }
        if (proxy != null && settings.useProxies) {
            log.info("Starting WebDriver with Proxy "+proxy.getIp());
            webDriver = WebDriverFactory.createWebDriver(log, this.context(), proxy);
        } else {
            log.info("Starting WebDriver without a Proxy");
            webDriver = WebDriverFactory.createWebDriver(log, this.context());
        }
    }

    private void crawl(String url, CheckedProxy proxy) throws WebDriverException {
        log.info("Started working on:" + url);

        // ToDo: Fix non-working proxies
        if (proxyChangeCounter > REQUESTS_PER_PROXY) {
            log.info("Changing Proxy");
            changeProxy(proxy);
            proxyChangeCounter = 0;
        } else {
            proxyChangeCounter++;
        }

        webDriver.get(url);
        HashSet<String> newUsers = new HashSet<>();

        //Wait for the page to load
        WebDriverWait webDriverWait = new WebDriverWait(webDriver,10);
        webDriverWait.until(ExpectedConditions.presenceOfElementLocated(By.id("page-container")));
        ((JavascriptExecutor) webDriver).executeScript("window.scrollTo(0, document.body.scrollHeight)");

        // Decide if you should skip, wait or scrape the page
        int initialTweetCount;
        try {
            initialTweetCount = getTweetCount();
        }
        catch(Exception e) {
            log.info("No Tweets for this day (No Such Element): " + url);
            return;
        }
        if(initialTweetCount == 1) {
            // If there is only one element, there is no data on this day
            log.info("No Tweets on this day for " + url);
            return;
        }
        else if(initialTweetCount > 7) {
            // There could be potentially more tweets, scrolling to find them
            while (true) {
                int tweetCount = getTweetCount();
                ((JavascriptExecutor) webDriver).executeScript("window.scrollTo(0, document.body.scrollHeight)");
                try {
                    Thread.sleep(dynamicContentWait);
                } catch (InterruptedException e) {
                    log.error(e, "Was interrupted during waiting for loading of website " + url);
                }
                if (tweetCount == getTweetCount()) {
                    log.info("Reached End of Page; Gathering Tweets");
                    break;
                }
            }
        }

        // Extract the tweets
        Document twPage = Jsoup.parse(webDriver.getPageSource());
        Elements tweets = twPage.select("ol[id=stream-items-id] li[data-item-type=tweet]");
        int foundTweets = 0;
        for (Element tweetHTML : tweets) {
            Element tweetDiv = tweetHTML.children().get(0);
            tweetDiv.children().select("div[class=content]");
            Tweet tweet = new Tweet(tweetDiv);
            newUsers.addAll(tweet.getReferenced_users());
            GraphStoreMaster.getSingleton(getContext()).tell(tweet.toSubGraph(), getSelf());
            foundTweets++;
        }
        log.info("Found " + newUsers.size() + " new users");
        MetricsListener.getRef(getContext()).tell(new MetricsListener.ScrapedTweetCount(foundTweets), getSelf());
        if (newUsers.size() > 0 && settings.extractUsers) {
            log.info("Found " + newUsers.size() + " new users");
            getSender().tell(new TwitterCrawlingScheduler.NewReference(newUsers), getSelf());
        }
    }

    private int getTweetCount() throws NoSuchElementException {
        return webDriver.findElement(By.id("stream-items-id")).findElements(By.tagName("li")).size();
    }

    private void handleActorIdentity(@NotNull ActorIdentity message) {
        // Watch the TwitterCrawlingScheduler to be notified of its death
        getContext().watch(message.getRef());
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        TwitterCrawlingScheduler.getSingleton(getContext()).tell(new Messages.RegisterMe(), getSelf());
        TwitterCrawlingScheduler.getSingleton(getContext()).tell(new Identify(1337), getSelf());
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
