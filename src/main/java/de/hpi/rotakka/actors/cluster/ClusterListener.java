package de.hpi.rotakka.actors.cluster;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.*;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class ClusterListener extends AbstractActor {
	
	public static final String DEFAULT_NAME = "clusterListener";

	public static Props props() {
		return Props.create(ClusterListener.class);
	}
	
	private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
	private final Cluster cluster = Cluster.get(this.context().system());
	
	@Override
	public void preStart() {
		this.cluster.subscribe(this.self(), MemberEvent.class, UnreachableMember.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}
	
	@Override
	public Receive createReceive() {
        return receiveBuilder()
                .match(CurrentClusterState.class, state -> this.log.info("Current members: {}", state.members()))
                .match(MemberUp.class, mUp -> this.log.info("Member is Up: {}", mUp.member()))
                .match(UnreachableMember.class, mUnreachable -> this.log.info("Member detected as unreachable: {}", mUnreachable.member()))
                .match(MemberRemoved.class, mRemoved -> this.log.info("Member is Removed: {}", mRemoved.member()))
                .match(MemberEvent.class, message -> {/* ignore */ })
                .build();
	}
}
