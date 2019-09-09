package de.hpi.rotakka.actors.proxy;

import com.sun.org.apache.xml.internal.security.utils.Base64;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Date;

@Data
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class CheckedProxy extends ProxyWrapper {
    public static final long serialVersionUID = 1L;
    Date lastChecked;

    // Manual Creation Constructor
    public CheckedProxy(String ip, int port, String protocol) {
        this.setIp(ip);
        this.setPort(port);
        this.setProtocol(protocol);
    }

    public CheckedProxy(@NotNull ProxyWrapper proxyWrapper) {
        this.setIp(proxyWrapper.getIp());
        this.setPort(proxyWrapper.getPort());
        this.setAverageResponseTime(proxyWrapper.getAverageResponseTime());
        this.setProtocol(proxyWrapper.getProtocol());
        this.setLastChecked(new Date(System.currentTimeMillis()));
    }

    public CheckedProxy(String seralizationString) {
        try {
            byte b[] = Base64.decode(seralizationString.getBytes());
            ByteArrayInputStream bi = new ByteArrayInputStream(b);
            ObjectInputStream si = new ObjectInputStream(bi);
            CheckedProxy obj = (CheckedProxy) si.readObject();

            this.setIp(obj.getIp());
            this.setPort(obj.getPort());
            this.setAverageResponseTime(obj.getAverageResponseTime());
            this.setProtocol(obj.getProtocol());
            this.setLastChecked(obj.getLastChecked());
        } catch (Exception e) {
            System.out.println(e);
        }

    }

    public String serialize() {
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            ObjectOutputStream so = new ObjectOutputStream(bo);
            so.writeObject(this);
            so.flush();
            return Base64.encode(bo.toByteArray());
        } catch (Exception e) {
            System.out.println(e);
        }
        return null;
    }
}
