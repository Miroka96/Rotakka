package de.hpi.rotakka;

import de.hpi.rotakka.actors.utils.RotakkaProxy;
import de.hpi.rotakka.actors.proxy.websites.CrawlerFreeProxyLists;

import java.util.List;

// This class can be used to test single components
public class MainTest {

    public static void main(String[] args) {

        // Test the Proxy Crawler
        CrawlerFreeProxyLists crawler = new CrawlerFreeProxyLists();
        List<RotakkaProxy> proxies = crawler.extract();
        System.out.println("Found proxies: "+proxies.size());

        // Test the new availablility check
//        try {
//            RotakkaProxy proxy = new RotakkaProxy("198.229.94.202", 25, "HTTP");
//            InetAddress address = InetAddress.getByName(proxy.getIp());
//            boolean reachable = address.isReachable(10000);
//            if (reachable) {
//                URLConnection connection = new URL("http://www.google.com").openConnection(proxy.getProxyObject());
//                connection.setConnectTimeout(10000);
//                connection.connect();
//                Object content = connection.getContent();
//                System.out.println("works!");
//            }
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
