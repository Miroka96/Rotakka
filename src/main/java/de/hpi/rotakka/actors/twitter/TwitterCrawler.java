package de.hpi.rotakka.actors.twitter;

import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractLoggingActor;
import de.hpi.rotakka.actors.utils.WebDriverFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.WebDriver;

import java.io.Serializable;

public class TwitterCrawler extends AbstractLoggingActor {

    public final static String DEFAULT_NAME = "twitterCrawler";

    public static Props props() {
        return Props.create(TwitterCrawler.class);
    }

    private static WebDriver webDriver;

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

    private void crawl(CrawlURL crawlUrl) {
        crawl(crawlUrl.url);
    }

    private void crawl(String url) {
        webDriver.get(url);

        //List<WebElement> tweets = webDriver.findElement(By.id("stream-items-id")).findElements(By.tagName("li"));
        Document twPage = Jsoup.parse(webDriver.getPageSource());
        Elements tweets = twPage.select("ol[id=stream-items-id] li[data-item-type=tweet]");

        for(Element tweet : tweets) {
            int i = 2;
            //tweet.childNodes.get(1).attributes()
            //tweet.select("div.tweet.js-stream-tweet.js-actionable-tweet.js-profile-popup-actionable.dismissible-content.original-tweet.js-original-tweet.tweet-has-context.has-cards.cards-forward")
        }
        int debug = 1;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        webDriver = WebDriverFactory.getWebDriver(log);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        webDriver.close();
    }

}
