package de.hpi.rotakka.actors.proxy.utility;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import scala.Int;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.logging.Logger;

@Data
@Getter
@AllArgsConstructor
public class RotakkaProxy {
    String ip;
    Integer port;
    String protocol;

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
