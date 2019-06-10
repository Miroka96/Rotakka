package de.hpi.rotakka.actors.proxy;

import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.logging.Logger;

@Data
@NoArgsConstructor
@Getter
public class ProxyWrapper implements Serializable {
    private String ip;
    private int port;
    private String protocol;
    @Setter
    private long averageResponseTime;

    public ProxyWrapper(String ip, int port, String protocol) {
        this.ip = ip;
        this.port = port;
        this.protocol = protocol;
    }

    /**
     * This method will return a java.net.RotakkaProxy object which is usually used when querying websites when
     * using java. The main thing happening here is the parsing of the protocol.
     */
    public java.net.Proxy getProxyObject() {
        Proxy.Type type;
        // HTTP, HTTPS, FTP
        if(protocol.equals("HTTP")) {
            type = Proxy.Type.HTTP;
        }
        // SOCKS4, SOCKS5
        else if(protocol.equals("SOCKS")) {
            type = Proxy.Type.SOCKS;
        } else {
            Logger.getGlobal().warning("Wrong protocol!");
            return null;
        }
        return new java.net.Proxy(type, new InetSocketAddress(ip, port));
    }
}
