package de.hpi.rotakka.actors.twitter;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ddata.*;
import de.hpi.rotakka.actors.AbstractReplicationActor;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.omg.CosNaming.NamingContextPackage.NotFound;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TwitterCrawlingScheduler extends AbstractReplicationActor {

    public static final String DEFAULT_NAME = "TwitterCrawlingScheduler";
    public static final String PROXY_NAME = DEFAULT_NAME + "Proxy";

    private final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();
    private final Cluster node = Cluster.get(getContext().getSystem());
    private final Key<ORSet<String>> newUsersKey = ORSetKey.create("new_users");
    private final ArrayList<String> entryPoints = new ArrayList<>(Arrays.asList("elonmusk","realDonaldTrump", "HPI_DE", "HillaryClinton", "ladygaga"));
    private final static String TWITTER_ADVANCED_URL = "https://twitter.com/search?l=&q=from%%3A%s%%20since%%3A%s%%20until%%3A%s";

    private ArrayList<ActorRef> awaitingWork = new ArrayList<>();
    private ArrayList<ActorRef> workers = new ArrayList<>();
    private LinkedList<String> workQueue = new LinkedList<>();
    private ArrayList<String> scrapedUsers = new ArrayList<>();

    public static Props props() {
        return Props.create(TwitterCrawlingScheduler.class);
    }

    public static ActorSelection getSingleton(akka.actor.ActorContext context) {
        return context.actorSelection("/user/" + PROXY_NAME);
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static final class NewReference implements Serializable {
        public static final long serialVersionUID = 1L;
        public HashSet<String> references;
    }

    @Data
    @AllArgsConstructor
    public static final class FinishedWork implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(NewReference.class, this::handleNewReference)
                .match(FinishedWork.class, this::handleFinishedWork)
                .match(Replicator.GetSuccess.class, this::handleReplicatorMessages)
                .match(Replicator.GetFailure.class, m -> log.error("Replicator couldn't get our data"))
                .match(NotFound.class, m -> log.error("Replicator couldn't find key"))
                .match(Messages.RegisterMe.class, this::handleRegisterMe)
                .build();
    }

    @Override
    public void preStart() {
        for(String twitterUser : entryPoints) {
            workQueue.addAll(createCrawlingLinks(twitterUser, 2018, 2018));
            scrapedUsers.add(twitterUser);
        }
        log.info("Generated "+workQueue.size()+" work packets");
    }

    private void handleRegisterMe(Messages.RegisterMe message) {
        // ToDO: Error handling if set is empty
        workers.add(getSender());
        getSender().tell(new TwitterCrawler.CrawlURL(workQueue.get(0)), this.getSelf());
        workQueue.remove(0);
    }

    // Add retweeted users & mentions to the data replicator to be crawled
    private void handleNewReference(NewReference message) {
        for(String user : message.getReferences()) {
            if(!scrapedUsers.contains(user)) {
                workQueue.addAll(createCrawlingLinks(user, 2018, 2018));
                scrapedUsers.add(user);
                log.info("Current Work Queue Size: "+workQueue.size());

                Replicator.Update<ORSet<String>> update = new Replicator.Update<>(
                        newUsersKey,
                        ORSet.create(),
                        Replicator.writeLocal(),
                        curr -> curr.add(node, user));
                replicator.tell(update, getSelf());
            }
        }
    }

    private void handleFinishedWork(FinishedWork message) {
        // final Replicator.ReadConsistency readNewUsers = new Replicator.ReadMajority(Duration.ofSeconds(5));
        // replicator.tell(new Replicator.Get<>(newUsersKey, readNewUsers), getSelf());
        // awaitingWork.add(getSender());
        if(workQueue.size() > 0) {
            getSender().tell(new TwitterCrawler.CrawlURL(workQueue.pop()), getSelf());
        }
        else {
            log.error("NO MORE WORK AVAILABLE; SHUTTING DOWN SYSTEM");
            context().system().terminate();
        }
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
                waitingActor.tell(new TwitterCrawler.CrawlURL(nextUser), this.getSelf());
            }
        }
        else {
            log.error("Could not handle Successful replicaor Messag");
        }
    }

    private ArrayList<String> createCrawlingLinks(String userID, int startYear, int endYear) {
        ArrayList<String> crawlingLinks = new ArrayList<>();

        // Generate Possible start & end dates
        for(int year = startYear; year <= endYear; year++) {
            for(int month = 1; month<=12; month++) {
                String monthString;
                if(month < 10) {
                    monthString = "0" + month;
                }
                else {
                    monthString = Integer.toString(month);
                }
                String startDate = year + "-" + monthString + "-01";
                LocalDate convertedDate = LocalDate.parse(startDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                convertedDate = convertedDate.withDayOfMonth(convertedDate.getMonth().length(convertedDate.isLeapYear()));
                String endDate = year + "-" + monthString + "-" + convertedDate.getDayOfMonth();
                String crawlingLink = String.format(TWITTER_ADVANCED_URL, userID, startDate, endDate);
                crawlingLinks.add(crawlingLink);
            }
        }
        return crawlingLinks;
    }


}
