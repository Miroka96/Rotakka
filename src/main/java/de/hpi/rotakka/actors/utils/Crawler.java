package de.hpi.rotakka.actors.utils;

import com.gargoylesoftware.htmlunit.WebClient;
import com.gargoylesoftware.htmlunit.html.HtmlPage;
import de.hpi.rotakka.actors.proxy.ProxyWrapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Level;

public abstract class Crawler {
    WebClient webClient;

    public Crawler() {
        webClient = new WebClient();
        java.util.logging.Logger.getLogger("com.gargoylesoftware").setLevel(Level.OFF);
    }

    // Extract the infos from a specific website
    public abstract List<ProxyWrapper> extract();

    protected Document get(String url) {
        Document doc = new Document("");
        try {
            HtmlPage page = webClient.getPage(url);
            doc = Jsoup.parse(new String(page.getWebResponse().getContentAsString().getBytes(), StandardCharsets.UTF_8));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return doc;
    }

}
