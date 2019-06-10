package de.hpi.rotakka.actors   .proxy.checking;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.rotakka.actors.AbstractReplicationActor;
import de.hpi.rotakka.actors.proxy.ProxyWrapper;
import de.hpi.rotakka.actors.utils.Messages;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.*;

public class ProxyCheckingScheduler extends AbstractReplicationActor {

    public static final String DEFAULT_NAME = "proxyCheckingScheduler";
    public static final String PROXY_NAME = DEFAULT_NAME + "Proxy";

    public static ActorSelection getSingleton(akka.actor.ActorContext context) {
        return context.actorSelection("/user/" + PROXY_NAME);
    }

    public static Props props() {
        return Props.create(ProxyCheckingScheduler.class);
    }

    @Data
    @EqualsAndHashCode(callSuper = true)
    @NoArgsConstructor
    public static class CheckedProxy extends ProxyWrapper {
        Date lastChecked;
    }

    @AllArgsConstructor
    public static class ProxyInProgress {
        ProxyWrapper proxy;
        Date started;
        ActorRef worker;

        ProxyInProgress(ProxyWrapper proxy, ActorRef worker) {
            this(proxy, new Date(), worker);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.RegisterMe.class, this::add)
                .match(Messages.UnregisterMe.class, this::remove)
                .match(ProxyWrapper.class, this::add)
                .match(CheckedProxy.class, this::add)
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
    }

    private Queue<ProxyWrapper> proxiesToCheck = new LinkedList<>();
    private HashMap<ActorRef, ProxyInProgress> proxiesInChecking = new HashMap<>();
    private HashSet<CheckedProxy> checkedProxies = new HashSet<>();

    private void add(ProxyWrapper proxy) {
        if (!availableWorkers.isEmpty()) {
            assign(proxy);
        } else {
            proxiesToCheck.add(proxy);
        }
    }

    private void add(CheckedProxy proxy) {
        free(getSender());
        proxiesInChecking.remove(getSender());
        boolean newlyAdded = checkedProxies.add(proxy);

        if (newlyAdded) {
            // TODO store proxy into replicator
        }
        if (!proxiesToCheck.isEmpty()) {
            ProxyWrapper work = proxiesToCheck.remove();
            assign(work);
        }
    }

    private void free(ActorRef worker) {
        busyWorkers.remove(worker);
        availableWorkers.add(worker);
    }

    private void assign(ProxyWrapper proxy) {
        ActorRef worker = availableWorkers.remove();
        busyWorkers.add(worker);
        ProxyInProgress progress = new ProxyInProgress(proxy, worker);
        proxiesInChecking.put(worker, progress);
        worker.tell(proxy, getSelf());
    }

    private void remove(Messages.UnregisterMe msg) {
        remove(getSender());
    }

    private void remove(ActorRef worker) {
        workers.remove(worker);
        availableWorkers.remove(worker);
        busyWorkers.remove(worker);
        if (proxiesInChecking.containsKey(worker)) {
            ProxyInProgress work = proxiesInChecking.remove(worker);
            proxiesToCheck.add(work.proxy);
        }
    }

}
