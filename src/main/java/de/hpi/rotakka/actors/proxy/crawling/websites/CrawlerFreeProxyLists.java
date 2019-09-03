package de.hpi.rotakka.actors.proxy.crawling.websites;

import akka.event.LoggingAdapter;
import de.hpi.rotakka.actors.proxy.ProxyWrapper;
import de.hpi.rotakka.actors.utils.Crawler;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;

// Not working because of extremly aggressive blocking
public class CrawlerFreeProxyLists extends Crawler {
    String baseURL = "http://www.freeproxylists.net/?page=";

    public CrawlerFreeProxyLists(LoggingAdapter loggingAdapter) {
        super(loggingAdapter);
    }

    @Override
    public List<ProxyWrapper> extract() {
        List<ProxyWrapper> proxies = new ArrayList<>();
        Boolean containsNext = true;
        int pageNumber = 1;

        while(containsNext) {
            System.out.println("Accessing Page "+pageNumber);
            Document doc = this.get(baseURL+pageNumber);
            if(!doc.select("div[class=page] a").text().contains("Next")) {
                containsNext = false;
            }
            Elements proxyElements = doc.select("table tr");
            proxyElements.remove(0);

            for(Element row : proxyElements) {
                String ip = row.select("a").text();
                int port = Integer.parseInt(row.select("td[align=center]").get(0).text());
                String protocol = row.select("td[align=center]").get(1).text();
                proxies.add(new ProxyWrapper(ip, port, protocol));
            }
        }
        webClient.close();
        return proxies;
    }

}
