package de.hpi.rotakka.actors;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.rotakka.Settings;
import de.hpi.rotakka.SettingsExtension;

public abstract class AbstractLoggingActor extends AbstractActor {
    protected LoggingAdapter log = Logging.getLogger(getContext().system(), this);
    protected final SettingsExtension settings = Settings.SettingsProvider.get(getContext().getSystem());
}
