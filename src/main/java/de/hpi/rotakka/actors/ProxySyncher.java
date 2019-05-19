package de.hpi.rotakka.actors;

import akka.actor.AbstractActor;
import akka.actor.Props;

import java.io.Serializable;

public class ProxySyncher extends AbstractActor {

    public static final String DEFAULT_NAME = "proxySyncher";

    public static Props props() {
        return Props.create(ProxySyncher.class);
    }

    public static final class GetProxy implements Serializable {}

    public static final class IntegrateNewProxies implements Serializable {}

    public static final class IntegrateCheckedProxies implements Serializable {}

    @Override
    public void preStart() {}

    @Override
    public void postStop() {}

    @Override
    public Receive createReceive() {
        return null;
    }

}
