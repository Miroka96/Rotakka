package de.hpi.rotakka.actors.proxy.crawling.websites;

import akka.event.LoggingAdapter;
import de.hpi.rotakka.actors.proxy.ProxyWrapper;
import de.hpi.rotakka.actors.utils.Crawler;
import de.hpi.rotakka.actors.utils.WebDriverFactory;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebDriverException;
import org.openqa.selenium.WebElement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CrawlerUsProxy extends Crawler {

    private static final List<String> BASE_URLS = Arrays.asList("https://www.socks-proxy.net/",
                                                                "https://free-proxy-list.net/",
                                                                "https://www.us-proxy.org/",
                                                                "https://free-proxy-list.net/uk-proxy.html",
                                                                "https://free-proxy-list.net/anonymous-proxy.html",
                                                                "https://www.sslproxies.org/");
    private WebDriver webDriver;

    public CrawlerUsProxy(LoggingAdapter loggingAdapter) {
        super(loggingAdapter);
        webDriver = WebDriverFactory.createWebDriver(null, null);
    }

    @Override
    public List<ProxyWrapper> extract() throws WebDriverException {
        List<ProxyWrapper> proxies = new ArrayList<>();
        for(String url : BASE_URLS) {
            webDriver.get(url);
            while (true) {
                List<WebElement> tableRows = webDriver.findElement(By.id("proxylisttable")).findElement(By.tagName("tbody")).findElements(By.tagName("tr"));
                for (WebElement row : tableRows) {
                    String ip = row.findElements(By.tagName("td")).get(0).getText();
                    String port = row.findElements(By.tagName("td")).get(1).getText();
                    if(!url.equals("https://www.socks-proxy.net/")) {
                        String https = row.findElement(By.className("hx")).getText();
                        // ToDo: Add functionality to differentiate between HTTP & HTTPS
                        if (https.equals("yes")) {
                            proxies.add(new ProxyWrapper(ip, Integer.parseInt(port), "HTTP"));
                        } else {
                            proxies.add(new ProxyWrapper(ip, Integer.parseInt(port), "HTTP"));
                        }
                    }
                    else {
                        proxies.add(new ProxyWrapper(ip, Integer.parseInt(port), "SOCKS"));
                    }
                }
                WebElement nextButton = webDriver.findElement(By.id("proxylisttable_next"));
                if (!nextButton.getAttribute("class").contains("disabled")) {
                    nextButton.findElement(By.tagName("a")).click();
                } else {
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        webDriver.close();
        return proxies;
    }
}
