package de.hpi.rotakka.actors.proxy.websites;

import de.hpi.rotakka.actors.proxy.utility.Crawler;
import de.hpi.rotakka.actors.proxy.utility.RotakkaProxy;
import org.apache.commons.codec.binary.Base64;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;

// Not working because of extremly aggressive blocking
public class CrawlerFreeProxyLists extends Crawler {
    String baseURL = "http://www.freeproxylists.net/?page=";

    @Override
    public List<RotakkaProxy> extract() {
        List<RotakkaProxy> proxies = new ArrayList<>();
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
                proxies.add(new RotakkaProxy(ip, port, protocol));
            }
        }
        return proxies;
    }

}
