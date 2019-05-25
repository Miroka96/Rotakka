package de.hpi.rotakka.actors.proxy.utility;

import lombok.Getter;
import lombok.Setter;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.logging.Logger;

@Getter
public class RotakkaProxy {
    private final String ip;
    private final int port;
    private final String protocol;
    @Setter private long averageResponseTime;

    public RotakkaProxy(String ip, int port, String protocol) {
        this.ip = ip;
        this.port = port;
        this.protocol = protocol;
    }

    public java.net.Proxy getProxyObject() {
        Proxy.Type type;
        // HTTP, HTTPS, FTP
        if(protocol.equals("HTTP")) {
            type = Proxy.Type.HTTP;
        }
        // SOCKS4, SOCKS5
        else if(protocol.equals("SOCKS")) {
            type = Proxy.Type.SOCKS;
        }
        else {
            Logger.getGlobal().warning("Wrong protocol!");
            return null;
        }
        return new java.net.Proxy(type, new InetSocketAddress(ip, port));
    }
}
