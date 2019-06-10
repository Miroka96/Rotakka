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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TwitterCrawler extends AbstractLoggingActor {

    public final static String DEFAULT_NAME = "twitterCrawler";
    private final static String TWITTER_BASE_URL = "https://twitter.com/";
    private final static String TWITTER_ADVANCED_URL = "https://twitter.com/search?l=&q=from%%3A%s%%20since%%3A%s%%20until%%3A%s";

    public static Props props() {
        return Props.create(TwitterCrawler.class);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class CrawlUser implements Serializable {
        public static final long serialVersionUID = 1L;
        String userID;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CrawlUser.class, this::handleCrawlUser)
                .build();
    }

    private static WebDriver webDriver;
    private List<Tweet> extractedTweets;

    private void handleCrawlUser(CrawlUser message) {
        crawl(message.userID);
    }

    private void crawl(String userID) {
        // ToDo: Remove blocking by sending self messaged and splitting work
        log.info("Started working on:" + userID);

        ArrayList<Integer> years = new ArrayList<>(Arrays.asList(2018, 2017));

        ArrayList<String> startDates = new ArrayList<>();
        ArrayList<String> endDates = new ArrayList<>();

        // Generate Possible start & end dates
        for(Integer year : years) {
            for(int month = 1; month<=12; month++) {
                String monthString;
                if(month < 10) {
                    monthString = "0" + month;
                }
                else {
                    monthString = Integer.toString(month);
                }
                String startDate = year + "-" + monthString + "-01";
                LocalDate convertedDate = LocalDate.parse(startDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                convertedDate = convertedDate.withDayOfMonth(convertedDate.getMonth().length(convertedDate.isLeapYear()));
                String endDate = year + "-" + monthString + "-" + convertedDate.getDayOfMonth();
                startDates.add(startDate);
                endDates.add(endDate);
            }
        }

        // Generate possible links
        ArrayList<String> crawlingLinks = new ArrayList<>();
        for(int i = 0; i<startDates.size(); i++) {
            String crawlingLink = String.format(TWITTER_ADVANCED_URL, userID, startDates.get(i), endDates.get(i));
            crawlingLinks.add(crawlingLink);
        }
        for(String link : crawlingLinks) {
            webDriver.get(link);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            long previousPageLength;
            while(true) {
                previousPageLength = Jsoup.parse(webDriver.getPageSource()).text().length();
                ((JavascriptExecutor) webDriver).executeScript("window.scrollTo(0, document.body.scrollHeight)");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if(previousPageLength == Jsoup.parse(webDriver.getPageSource()).text().length()) {
                    log.info("Reached End of Page; Scraping");
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
            for (Tweet tweet : extractedTweets) {
                log.info(tweet.getTweet_text());
            }
        }


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
