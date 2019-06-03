package de.hpi.rotakka.actors.twitter;

import akka.actor.Props;
import de.hpi.rotakka.actors.utils.Tweet;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeOptions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static de.hpi.rotakka.actors.utils.Utility.createSeleniumWebDriver;

public class TwitterCrawler {

    public final static String DEFAULT_NAME = "twitter_crawler";

    public static Props props() {
        return Props.create(TwitterCrawler.class);
    }

    @Data
    @AllArgsConstructor
    public static final class GetProxies implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    private WebDriver webDriver;
    private List<Tweet> extracted_tweets;

    public TwitterCrawler() {
        ChromeOptions chromeOptions = new ChromeOptions();
        webDriver = createSeleniumWebDriver(false, chromeOptions);
        extracted_tweets = new ArrayList<>();
    }

    public void extract(String url) {
        webDriver.get(url);

        //List<WebElement> tweets = webDriver.findElement(By.id("stream-items-id")).findElements(By.tagName("li"));
        Document twPage = Jsoup.parse(webDriver.getPageSource());
        Elements tweets = twPage.select("ol[id=stream-items-id] li[data-item-type=tweet]");

        for(Element tweet : tweets) {
            Element tweetDiv = tweet.children().get(0);
            tweetDiv.children().select("div[class=content]");
            extracted_tweets.add(new Tweet(tweetDiv));
        }
    }
}
