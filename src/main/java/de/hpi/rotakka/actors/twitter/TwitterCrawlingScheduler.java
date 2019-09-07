package de.hpi.rotakka.actors.twitter;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ddata.*;
import de.hpi.rotakka.actors.AbstractReplicationActor;
import de.hpi.rotakka.actors.cluster.MetricsListener;
import de.hpi.rotakka.actors.proxy.CheckedProxy;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.omg.CosNaming.NamingContextPackage.NotFound;

import java.io.Serializable;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;


// ToDO:
// - Add UserQueue & ScrapedUsers & Workers into DataReplicator
// - Make a configuration file
// - Extension: Make userQueue a priority Queue
// - Extension: Crawling Depth
public class TwitterCrawlingScheduler extends AbstractReplicationActor {

    public static final String DEFAULT_NAME = "TwitterCrawlingScheduler";
    public static final String PROXY_NAME = DEFAULT_NAME + "Proxy";

    private final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();
    private final Cluster node = Cluster.get(getContext().getSystem());
    private final Key<ORSet<String>> newUsersKey = ORSetKey.create("new_users");
    private final Key<ORSet<String>> proxyListKey = ORSetKey.create("checkedProxyListKey");

    private final ArrayList<String> entryPoints = new ArrayList<>(Arrays.asList("S100D27", "elonmusk","realDonaldTrump", "HillaryClinton", "ladygaga"));
    private final static String TWITTER_ADVANCED_URL = "https://twitter.com/search?l=&q=from%%3A%s%%20since%%3A%s%%20until%%3A%s";

    private ArrayList<ActorRef> awaitingWork = new ArrayList<>();
    private ArrayList<ActorRef> workers = new ArrayList<>();
    private LinkedList<String> userQueue = new LinkedList<>();
    private LinkedList<String> workPackets = new LinkedList<>();
    private ArrayList<String> knownUsers = new ArrayList<>();
    private ArrayList<CheckedProxy> storedProxies = new ArrayList<>();

    private Date startDate;
    private Date endDate;

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
                .match(ArrayList.class, this::handleNewCheckedProxies)
                .build();
    }

    @Override
    public void preStart() {
        // Todo: Make Adaptive
        try {
            this.startDate = new SimpleDateFormat("dd-MM-yyyy").parse("01-05-2019");
            this.endDate = new SimpleDateFormat("dd-MM-yyyy").parse("01-06-2019");
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        userQueue.addAll(entryPoints);
        populateWorkPacketsQueueIfNecessary();

        storedProxies.add(null);
        log.info("Generated "+workPackets.size()+" work packets");
    }

    private void handleRegisterMe(Messages.RegisterMe message) {
        workers.add(getSender());
        populateWorkPacketsQueueIfNecessary();
        String workPacket = workPackets.pop();
        getSender().tell(new TwitterCrawler.CrawlURL(workPacket, storedProxies.get(new Random().nextInt(storedProxies.size()))), this.getSelf());
    }

    // Add retweeted users & mentions to the data replicator to be crawled
    private void handleNewReference(NewReference message) {
        for(String user : message.getReferences()) {
            if(!knownUsers.contains(user)) {
                userQueue.add(user);
                knownUsers.add(user);
                log.debug("Current Work Queue Size: " + userQueue.size());

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
        populateWorkPacketsQueueIfNecessary();
        final Replicator.ReadConsistency readMajority = new Replicator.ReadMajority(Duration.ofSeconds(5));
        replicator.tell(new Replicator.Get<>(proxyListKey, readMajority), getSelf());
        getSender().tell(new TwitterCrawler.CrawlURL(workPackets.pop(), storedProxies.get(new Random().nextInt(storedProxies.size()))), getSelf());
    }

    private void handleReplicatorMessages(Replicator.GetSuccess message) {
        // ToDo: This does not have any use as far as i see
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
                waitingActor.tell(new TwitterCrawler.CrawlURL(nextUser, storedProxies.get(new Random().nextInt(storedProxies.size()))), this.getSelf());
            }
        }
        if(message.key().equals(proxyListKey)) {
            log.info("Trying to deserialize Proxies");
            // Deserialize the Proxies and add them to the list
            Replicator.GetSuccess<ORSet<String>> getSuccessObject = message;
            Set<String> serializedProxySet = getSuccessObject.dataValue().getElements();
            if(serializedProxySet.size() > 0) {
                storedProxies = new ArrayList<>();
                for (String proxyString : serializedProxySet) {
                    storedProxies.add(new CheckedProxy(proxyString));
                }
                log.info("Successfully added proxies");
            }
            else {
                log.info("Replicator Set was empty, did not add anything");
            }
        }
        else {
            log.error("Could not handle replicator success message");
        }
    }

    private void handleNewCheckedProxies(ArrayList<CheckedProxy> proxyList) {
        for(CheckedProxy proxy : proxyList) {
            if(!storedProxies.contains(proxy)) {
                storedProxies.add(proxy);
            }
        }
    }

    private ArrayList<String> createCrawlingLinks(String userID) {
        ArrayList<String> crawlingLinks = new ArrayList<>();

        Calendar startCal = Calendar.getInstance();
        Calendar endCal = Calendar.getInstance();
        startCal.setTime(this.startDate);
        endCal.setTime(this.startDate);
        endCal.add(Calendar.DAY_OF_MONTH, 1);

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        while(startCal.getTime().before(this.endDate)) {
            String startDateString = dateFormat.format(startCal.getTime());
            String endDateString = dateFormat.format(endCal.getTime());

            String crawlingLink = String.format(TWITTER_ADVANCED_URL, userID, startDateString, endDateString);
            crawlingLinks.add(crawlingLink);

            startCal.add(Calendar.DAY_OF_MONTH, 1);
            endCal.add(Calendar.DAY_OF_MONTH, 1);
        }

        return crawlingLinks;
    }

    private void populateWorkPacketsQueueIfNecessary() {
        if(workPackets.isEmpty()) {
            if (!userQueue.isEmpty()) {
                String user = userQueue.pop();
                workPackets.addAll(createCrawlingLinks(user));
                knownUsers.add(user);
                MetricsListener.getSingleton(getContext()).tell(new MetricsListener.FinishedUser(), getSelf());
            } else {
                log.error("NO MORE WORK AVAILABLE; SHUTTING DOWN SYSTEM");
                context().system().terminate();
            }
        }
    }


}
