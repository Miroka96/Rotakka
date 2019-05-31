package de.hpi.rotakka;

import de.hpi.rotakka.actors.proxy.crawling.websites.CrawlerFreeProxyLists;

// This class can be used to test single components
public class MainTest {

    public static void main(String[] args) {

        CrawlerFreeProxyLists a = new CrawlerFreeProxyLists();
        a.extract();

//        // Test the new availablility check
//        try {
//            ProxyWrapper proxy = new ProxyWrapper("190.114.254.171", 8080, "HTTP");
//            InetAddress address = InetAddress.getByName(proxy.getIp());
//            boolean reachable = address.isReachable(10000);
//            System.out.println(reachable);
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
//
//        TwitterCrawler twC = new TwitterCrawler();
//        twC.extract("https://twitter.com/elonmusk");
//        // Test the RotakkarProxy Crawler
//        //CrawlerFreeProxyLists crawler = new CrawlerFreeProxyLists();
//        //&List<RotakkaProxy> proxies = crawler.extract();
//        //System.out.println("Found proxies: "+proxies.size());


    }
}
