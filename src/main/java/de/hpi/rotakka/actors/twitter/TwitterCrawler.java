package de.hpi.rotakka.actors.twitter;

import org.openqa.selenium.WebDriver;
import org.openqa.selenium.chrome.ChromeOptions;

import static de.hpi.rotakka.actors.utils.Utility.createSeleniumWebDriver;

public class TwitterCrawler {
    WebDriver webDriver;

    public TwitterCrawler() {
        ChromeOptions chromeOptions = new ChromeOptions();
        webDriver = createSeleniumWebDriver(false, chromeOptions);
    }

    public void testMe(String url) {
        webDriver.get(url);
    }
}
