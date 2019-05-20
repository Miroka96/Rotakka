package de.hpi.rotakka.actors;

import akka.actor.Props;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class Initiator extends LoggingActor {

    public static String DEFAULT_NAME = "initiator";

    public static Props props() {
        return Props.create(Initiator.class);
    }

    @Data
    @AllArgsConstructor
    public static final class RunConfiguration implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder().match(RunConfiguration.class, this::handle).build();
    }

    private void handle(RunConfiguration rc) {
        log.info("Here I am!");
    }


}
