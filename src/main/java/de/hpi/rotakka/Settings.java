package de.hpi.rotakka;

import akka.actor.AbstractExtensionId;
import akka.actor.ExtendedActorSystem;
import akka.actor.ExtensionIdProvider;




public class Settings extends AbstractExtensionId<SettingsExtension> implements ExtensionIdProvider {
    public static final Settings SettingsProvider = new Settings();

    private Settings() {}

    public Settings lookup() {
        return Settings.SettingsProvider;
    }

    public SettingsExtension createExtension(ExtendedActorSystem system) {
        try {
            return new SettingsExtension(system.settings().config());
        } catch (Exception e) {
            e.printStackTrace();
            system.terminate();
            return null;
        }
    }
}