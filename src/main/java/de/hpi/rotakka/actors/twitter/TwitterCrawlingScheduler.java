package de.hpi.rotakka.actors.twitter;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.ddata.DistributedData;
import de.hpi.rotakka.actors.AbstractReplicationActor;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class TwitterCrawlingScheduler extends AbstractReplicationActor {

    public static final String DEFAULT_NAME = "websiteCrawlingScheduler";
    private final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();

    public static Props props() {
        return Props.create(TwitterCrawlingScheduler.class);
    }

    @Data
    @AllArgsConstructor
    public static final class NewReference implements Serializable {
        public static final long serialVersionUID = 1L;
        public String[] mentions;
        public String retweeted_user;
    }

    @Data
    @AllArgsConstructor
    public static final class FinishedUser implements Serializable {
        public static final long serialVersionUID = 1L;
        public ActorRef actor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(NewReference.class, this::handleNewReference)
                .match(FinishedUser.class, this::handleFinishedUser)
                .build();
    }

    private void handleNewReference(NewReference message) {
        // ToDo
    }

    private void handleFinishedUser(FinishedUser message) {
        // ToDo
    }


}
