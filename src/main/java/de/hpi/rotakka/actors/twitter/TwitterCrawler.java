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
import java.util.HashSet;
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
    private List<Tweet> extractedTweets = new ArrayList<>();

    private void handleCrawlURL(CrawlURL message) {
        crawl(message.getUrl());
        getSender().tell(new TwitterCrawlingScheduler.FinishedWork(), getSelf());
    }

    private void crawl(String url) {
        log.info("Started working on:" + url);
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
            extractedTweets.add(tweet);
        }
        log.info("Scraped "+extractedTweets.size()+" tweets");
        log.info("Found "+newUsers.size()+" new users");
        // ToDo: Send the scraped tweets to the graph store
        // ToDo: Send the mentions to the TwitterCrawlingScheduler
        if(newUsers.size() > 0) {
            log.info("Found "+newUsers.size()+" new users");
            getSender().tell(new TwitterCrawlingScheduler.NewReference((String[]) newUsers.toArray()), getSelf());
        }
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        webDriver = WebDriverFactory.createWebDriver(log, this.context());
        TwitterCrawlingScheduler.getSingleton(context()).tell(new Messages.RegisterMe(), getSelf());
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        if (webDriver != null) {
            webDriver.close();
        }
    }

}
