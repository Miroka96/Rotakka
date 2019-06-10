package de.hpi.rotakka.actors.data.graph;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import de.hpi.rotakka.actors.utils.Messages;

import java.util.ArrayList;

public class GraphStoreMaster extends AbstractGraphStore {

    public static final String DEFAULT_NAME = "graphStoreMaster";
    public static final String PROXY_NAME = DEFAULT_NAME + "Proxy";

    public static ActorSelection getSingleton(akka.actor.ActorContext context) {
        return context.actorSelection("/user/" + PROXY_NAME);
    }

    public static Props props() {
        return Props.create(GraphStoreMaster.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SubGraph.class, this::add)
                .match(Vertex.class, this::add)
                .match(Edge.class, this::add)
                .match(Messages.RegisterMe.class, this::add)
                .build();
    }

    @Override
    void add(Vertex vertex) {

    }

    @Override
    void add(Edge edge) {

    }

    private ArrayList<ActorRef> slaves = new ArrayList<>();

    void add(Messages.RegisterMe slave) {
        add(getSender());
    }

    void add(ActorRef slave) {
        slaves.add(slave);
    }

}
