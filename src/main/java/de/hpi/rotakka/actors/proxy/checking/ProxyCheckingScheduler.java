package de.hpi.rotakka.actors   .proxy.checking;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.cluster.ddata.*;
import de.hpi.rotakka.actors.AbstractReplicationActor;
import de.hpi.rotakka.actors.proxy.CheckedProxy;
import de.hpi.rotakka.actors.proxy.ProxyWrapper;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.*;

// ToDo: Add worker references to DataReplicator
public class ProxyCheckingScheduler extends AbstractReplicationActor {

    public static final String DEFAULT_NAME = "proxyCheckingScheduler";
    public static final String PROXY_NAME = DEFAULT_NAME + "Proxy";
    public static final int PROXY_SAMPLE_SIZE = 5;

    private Queue<ProxyWrapper> proxiesToCheck = new LinkedList<>();
    private HashMap<ActorRef, GiveCheckedProxySample> awaitingCheckedProxies = new HashMap<>();
    private HashSet<CheckedProxy> checkedProxies = new HashSet<>();

    private final ActorRef replicator = DistributedData.get(getContext().getSystem()).replicator();
    private final Key<ORSet<String>> dataKey = ORSetKey.create("checked_proxy_list");
    private final SelfUniqueAddress selfUniqueAddress = DistributedData.get(getContext().getSystem()).selfUniqueAddress();

    public static ActorSelection getSingleton(akka.actor.ActorContext context) {
        return context.actorSelection("/user/" + PROXY_NAME);
    }

    public static Props props() {
        return Props.create(ProxyCheckingScheduler.class);
    }

    public enum ProxyStatus {
        REACHABLE,
        UNREACHABLE
    }

    @Data
    @AllArgsConstructor
    public static final class GetWork implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Data
    @AllArgsConstructor
    public static final class GiveCheckedProxySample implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Data
    @AllArgsConstructor
    public static final class IntegrateCheckedProxy implements Serializable {
        public static final long serialVersionUID = 1L;
        public static CheckedProxy checkedProxy;

        public IntegrateCheckedProxy(CheckedProxy proxy) {
            checkedProxy = proxy;
        }

        public CheckedProxy getCheckedProxy() {
            return checkedProxy;
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.RegisterMe.class, this::add)
                .match(Messages.UnregisterMe.class, this::remove)
                .match(ProxyWrapper.class, this::add)
                .match(CheckedProxy.class, this::add)
                .match(GetWork.class, this::handleGetWork)
                .match(IntegrateCheckedProxy.class, this::handleIntegrateCheckedProxy)
                .match(GiveCheckedProxySample.class, this::handleGiveCheckedProxySample)
                .build();
    }

    private HashSet<ActorRef> workers = new HashSet<>();
    private Queue<ActorRef> availableWorkers = new LinkedList<>();
    private HashSet<ActorRef> busyWorkers = new HashSet<>();

    private void add(Messages.RegisterMe msg) {
        add(getSender());
    }

    private void add(ActorRef worker) {
        workers.add(worker);
        availableWorkers.add(worker);
    }

    private void add(ProxyWrapper proxy) {
        List<ProxyWrapper> proxyList = new ArrayList<>();
        proxyList.add(proxy);
        if (!availableWorkers.isEmpty()) {
            assign(proxyList);
        } else {
            proxiesToCheck.add(proxy);
        }
    }

    private void handleGetWork(GetWork msg) {
        assignWork();
    }

    private void handleIntegrateCheckedProxy(IntegrateCheckedProxy msg) {
        // For some reason this extra message was necessary instead of just taking the CheckedProxy Object
        boolean newlyAdded = checkedProxies.add(msg.getCheckedProxy());
        if(newlyAdded) {
            log.info("Trying to add CheckedProxies to the DataReplicator");
            Replicator.Update<ORSet<String>> update = new Replicator.Update<>(
                    dataKey,
                    ORSet.create(),
                    Replicator.writeLocal(),
                    curr -> curr.add(selfUniqueAddress, msg.getCheckedProxy().serialize()));
            replicator.tell(update, getSelf());
        }
        if(awaitingCheckedProxies.size() > 0) {
            for(ActorRef key : awaitingCheckedProxies.keySet()) {
                handleGiveCheckedProxySample(awaitingCheckedProxies.get(key));
            }
        }
        assignWork();
    }

    private void handleGiveCheckedProxySample(GiveCheckedProxySample msg) {
        if(checkedProxies.size() == 0) {
            awaitingCheckedProxies.put(getSender(), msg);
        }
        else {
            CheckedProxy[] checkedProxiesArray = (CheckedProxy[]) checkedProxies.toArray();
            ArrayList<CheckedProxy> sample = new ArrayList<>();
            for (int i = 0; i < PROXY_SAMPLE_SIZE; i++) {
                int rnd = new Random().nextInt(checkedProxiesArray.length);
                sample.add(checkedProxiesArray[rnd]);
            }
            sender().tell(sample, getSelf());
        }
    }

    private void remove(Messages.UnregisterMe msg) {
        remove(getSender());
    }

    private void remove(ActorRef worker) {
        workers.remove(worker);
        availableWorkers.remove(worker);
        busyWorkers.remove(worker);
    }

    private void assign(List<ProxyWrapper> proxyList) {
        ActorRef worker = availableWorkers.remove();
        busyWorkers.add(worker);
        worker.tell(new ProxyChecker.CheckProxies(proxyList), getSelf());
    }


    private void assignWork() {
        busyWorkers.remove(getSender());
        availableWorkers.add(getSender());

        if (!proxiesToCheck.isEmpty()) {
            ArrayList<ProxyWrapper> proxyList = new ArrayList<>();
            int listLength = 5;
            while(proxiesToCheck.size() > 0 && listLength > 0) {
                proxyList.add(proxiesToCheck.remove());
                listLength--;
            }
            log.info("Assigned Work upon hearing back; Current Checking Queue: " + proxiesToCheck.size());
            assign(proxyList);
        }
        else {
            log.info("No work available to distribute");
        }
    }

}
