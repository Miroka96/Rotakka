package de.hpi.rotakka;

import de.hpi.rotakka.actors.proxy.utility.Proxy;
import de.hpi.rotakka.actors.proxy.websites.CrawlerFreeProxyCZ;

import java.util.List;

// This class can be used to test single components
public class MainTest {

    public static void main(String[] args) {
        CrawlerFreeProxyCZ crawler = new CrawlerFreeProxyCZ();

        // This works and gives back ~90 Proxies
        List<Proxy> proxies = crawler.extract("Rework abstract class, since we dont need a url when the page is clear");
    }
}
