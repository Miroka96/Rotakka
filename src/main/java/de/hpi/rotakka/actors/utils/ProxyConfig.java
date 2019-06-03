package de.hpi.rotakka.actors.utils;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public final class ProxyConfig {
    String ip;
    int port;
}
