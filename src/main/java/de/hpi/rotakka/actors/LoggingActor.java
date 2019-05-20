package de.hpi.rotakka.actors;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public abstract class LoggingActor extends AbstractActor {
    protected LoggingAdapter log = Logging.getLogger(getContext().system(), this);
}
