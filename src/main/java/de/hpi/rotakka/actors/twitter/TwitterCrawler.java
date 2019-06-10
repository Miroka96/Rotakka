package de.hpi.rotakka.actors.twitter;

import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.utils.Messages;
import de.hpi.rotakka.actors.utils.Tweet;
import de.hpi.rotakka.actors.utils.WebDriverFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.WebDriver;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TwitterCrawler extends AbstractLoggingActor {

    public final static String DEFAULT_NAME = "twitterCrawler";
    private final static int PAGE_LOAD_WAIT = 2000;
    private final static int PAGE_AJAX_WAIT = 2000;

    public static Props props() {
        return Props.create(TwitterCrawler.class);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class CrawlURL implements Serializable {
        public static final long serialVersionUID = 1L;
        String url;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CrawlURL.class, this::handleCrawlURL)
                .build();
    }

    private static WebDriver webDriver;
    private List<Tweet> extractedTweets;

    private void handleCrawlURL(CrawlURL message) {
        crawl(message.getUrl());
        getSender().tell(new TwitterCrawlingScheduler.FinishedWork(), getSelf());
    }

    private void crawl(String url) {
        log.info("Started working on:" + url);
        webDriver.get(url);

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
        for (Element tweet : tweets) {
            Element tweetDiv = tweet.children().get(0);
            tweetDiv.children().select("div[class=content]");
            extractedTweets.add(new Tweet(tweetDiv));
        }
        log.info("Scraped "+extractedTweets.size()+" tweets");
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        webDriver = WebDriverFactory.createWebDriver(log, this.context());
        extractedTweets = new ArrayList<>();
        context().actorSelection("/user/" + TwitterCrawlingScheduler.DEFAULT_NAME + "Proxy").tell(new Messages.RegisterMe(), getSelf());
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        if (webDriver != null) {
            webDriver.close();
        }
    }

}
