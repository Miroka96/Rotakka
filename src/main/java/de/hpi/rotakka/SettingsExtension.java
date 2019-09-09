package de.hpi.rotakka;

import akka.actor.Extension;
import com.typesafe.config.Config;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class SettingsExtension implements Extension {

    public Date startDate = null;
    public Date endDate = null;
    public List<String> entryPointUsers;
    public int requestPerProxy;

    public SettingsExtension(Config config) {
        entryPointUsers =  new ArrayList<>(Arrays.asList(config.getString("rotakka.twittercrawling.entryPointUsers").split(",")));
        requestPerProxy = config.getInt("rotakka.proxycrawling.requestPerProxy");

        try {
            startDate = new SimpleDateFormat("dd-MM-yyyy").parse(config.getString("rotakka.twittercrawling.startDate"));
            endDate = new SimpleDateFormat("dd-MM-yyyy").parse(config.getString("rotakka.twittercrawling.endDate"));
        }
        catch (ParseException e) {
            e.printStackTrace();
        }
    }
}