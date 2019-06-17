package de.hpi.rotakka.actors.proxy;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class CheckedProxy extends ProxyWrapper {
    Date lastChecked;

    public CheckedProxy(ProxyWrapper proxyWrapper) {
        this.setIp(proxyWrapper.getIp());
        this.setPort(proxyWrapper.getPort());
        this.setAverageResponseTime(proxyWrapper.getAverageResponseTime());
        this.setProtocol(proxyWrapper.getProtocol());
        this.setLastChecked(new Date(System.currentTimeMillis()));
    }
}
