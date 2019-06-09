package de.hpi.rotakka.actors.twitter;

import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.utils.Tweet;
import de.hpi.rotakka.actors.utils.WebDriverFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.WebDriver;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class TwitterCrawler extends AbstractLoggingActor {

    public final static String DEFAULT_NAME = "twitterCrawler";

    public static Props props() {
        return Props.create(TwitterCrawler.class);
    }

    @Data
    @AllArgsConstructor
    public static final class CrawlURL implements Serializable {
        public static final long serialVersionUID = 1L;
        String url;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CrawlURL.class, this::crawl)
                .build();
    }

    private static WebDriver webDriver;
    private List<Tweet> extracted_tweets;

    private void crawl(CrawlURL crawlUrl) {
        crawl(crawlUrl.url);
    }

    public void crawl(String url) {
        webDriver.get(url);
        Document twPage = Jsoup.parse(webDriver.getPageSource());
        Elements tweets = twPage.select("ol[id=stream-items-id] li[data-item-type=tweet]");

        for(Element tweet : tweets) {
            Element tweetDiv = tweet.children().get(0);
            tweetDiv.children().select("div[class=content]");
            extracted_tweets.add(new Tweet(tweetDiv));
        }
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        webDriver = WebDriverFactory.createWebDriver(log, this.context());
        extracted_tweets = new ArrayList<>();
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        webDriver.close();
    }

}
