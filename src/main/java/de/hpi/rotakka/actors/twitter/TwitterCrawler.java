package de.hpi.rotakka.actors.twitter;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeOptions;

import static de.hpi.rotakka.actors.utils.Utility.createSeleniumWebDriver;

public class TwitterCrawler {
    WebDriver webDriver;

    public TwitterCrawler() {
        ChromeOptions chromeOptions = new ChromeOptions();
        webDriver = createSeleniumWebDriver(false, chromeOptions);
    }

    public void extract(String url) {
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
}
