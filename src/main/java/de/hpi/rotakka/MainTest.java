package de.hpi.rotakka;

import de.hpi.rotakka.actors.proxy.utility.RotakkaProxy;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.URLConnection;

// This class can be used to test single components
public class MainTest {

    public static void main(String[] args) {
        //CrawlerFreeProxyCZ crawler = new CrawlerFreeProxyCZ();

        // This works and gives back ~90 Proxies
        //List<RotakkaProxy> proxies = crawler.extract();
        try {
            RotakkaProxy proxy = new RotakkaProxy("192.33.31.130", 80, "HTTP");
            InetAddress address = InetAddress.getByName(proxy.getIp());
            boolean reachable = address.isReachable(10000);
            if (reachable) {
                URLConnection connection = new URL("http://www.google.com").openConnection(proxy.getProxyObject());
                connection.setConnectTimeout(10000);
                connection.connect();
                Object content = connection.getContent();
                System.out.println("works!");
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
