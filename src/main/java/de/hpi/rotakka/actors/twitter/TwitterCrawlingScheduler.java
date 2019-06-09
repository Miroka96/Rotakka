package de.hpi.rotakka.actors.twitter;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ddata.*;
import de.hpi.rotakka.actors.AbstractReplicationActor;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.omg.CosNaming.NamingContextPackage.NotFound;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;

public class TwitterCrawlingScheduler extends AbstractReplicationActor {

    public static final String DEFAULT_NAME = "websiteCrawlingScheduler";
    private final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();
    private final Cluster node = Cluster.get(getContext().getSystem());
    private final Key<ORSet<String>> newUsersKey = ORSetKey.create("new_users");
    private final ArrayList<String> entryPoints = new ArrayList<>(Arrays.asList("realDonaldTrump", "HPI_DE"));

    private ArrayList<ActorRef> awaitingWork = new ArrayList<>();
    private ArrayList<ActorRef> workers = new ArrayList<>();

    public static Props props() {
        return Props.create(TwitterCrawlingScheduler.class);
    }

    @Data
    @AllArgsConstructor
    public static final class NewReference implements Serializable {
        public static final long serialVersionUID = 1L;
        public String[] references;
    }

    @Data
    @AllArgsConstructor
    public static final class FinishedUser implements Serializable {
        public static final long serialVersionUID = 1L;
        public ActorRef sendingActor;
    }

    @Data
    @AllArgsConstructor
    public static final class RegisterMe implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(NewReference.class, this::handleNewReference)
                .match(FinishedUser.class, this::handleFinishedUser)
                .match(Replicator.GetSuccess.class, this::handleReplicatorMessages)
                .match(Replicator.GetFailure.class, m -> log.error("Replicator couldn't get our data"))
                .match(NotFound.class, m -> log.error("Replicator couldn't find key"))
                .match(RegisterMe.class,  this::handleRegisterMe)
                .build();
    }

    private void handleRegisterMe(RegisterMe message) {
        // ToDO: Error handling if set is empty
        workers.add(getSender());
        getSender().tell(new TwitterCrawler.CrawlUser(entryPoints.get(0)), this.getSelf());
        entryPoints.remove(0);
    }

    // Add retweeted users & mentions to the data replicator to be crawled
    private void handleNewReference(NewReference message) {
        for(String user : message.getReferences()) {
            Replicator.Update<ORSet<String>> update = new Replicator.Update<>(
                    newUsersKey,
                    ORSet.create(),
                    Replicator.writeLocal(),
                    curr -> curr.add(node, user));
            replicator.tell(update, getSelf());
        }
    }

    private void handleFinishedUser(FinishedUser message) {
        final Replicator.ReadConsistency readNewUsers = new Replicator.ReadMajority(Duration.ofSeconds(5));
        replicator.tell(new Replicator.Get<>(newUsersKey, readNewUsers), getSelf());
        awaitingWork.add(message.sendingActor);
    }

    private void handleReplicatorMessages(Replicator.GetSuccess message) {
        if(message.key().equals(newUsersKey)) {
            Replicator.GetSuccess<ORSet<String>> getSuccessObject = message;
            Set<String> newUserSet = getSuccessObject.dataValue().getElements();
            if(awaitingWork.size() > 0) {
                ActorRef waitingActor = awaitingWork.get(0);
                String nextUser = newUserSet.iterator().next();
                Replicator.Update<ORSet<String>> update = new Replicator.Update<>(
                        newUsersKey,
                        ORSet.create(),
                        Replicator.writeLocal(),
                        curr -> curr.remove(node, nextUser));
                replicator.tell(update, getSelf());
                waitingActor.tell(new TwitterCrawler.CrawlUser(nextUser), this.getSelf());
            }
        }
        else {
            log.error("Could not handle Successful replicaor Messag");
        }
    }


}
