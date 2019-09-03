package de.hpi.rotakka.actors.proxy.crawling.websites;

import akka.event.LoggingAdapter;
import de.hpi.rotakka.actors.proxy.ProxyWrapper;
import de.hpi.rotakka.actors.utils.Crawler;
import org.apache.commons.codec.binary.Base64;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.util.ArrayList;
import java.util.List;

public class CrawlerFreeProxyCZ extends Crawler {
    String baseURL = "http://free-proxy.cz/en/proxylist/main/";
    // They do have CAPTCHA if one goes over page 6, be aware of that
    // Proof of concept class

    public CrawlerFreeProxyCZ(LoggingAdapter loggingAdapter) {
        super(loggingAdapter);
    }

    @Override
    public List<ProxyWrapper> extract() {
        List<ProxyWrapper> proxies = new ArrayList<>();

        for(int i = 1; i < 4; i++) {
            String nextPage = this.baseURL+i;
            try {
                Document doc = this.get(nextPage);
                if (doc.html().length() > 0) {
                    Elements elements = doc.select("table[id=proxy_list] tr");
                    elements.remove(0);
                    for (Element trElement : elements) {
                        if (trElement.select("td").size() > 5) {
                            String base_64_ip = trElement.select("td[style=\"text-align:center\"] script").html().split("\"")[1].replaceAll("\"", "");
                            Base64 base64 = new Base64();
                            String ip = new String(base64.decode(base_64_ip.getBytes()));
                            int port = Integer.parseInt(trElement.select("span[class=fport]").text());
                            proxies.add(new ProxyWrapper(ip, port, "HTTP"));
                        }
                    }
                }
            }
            catch(Exception e) {
                // ToDo: Fix me
            }
        }
        webClient.close();
        return proxies;
    }
}
