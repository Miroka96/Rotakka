package de.hpi.rotakka;

import akka.actor.Extension;
import com.typesafe.config.Config;
import org.jetbrains.annotations.NotNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class SettingsExtension implements Extension {

    public final Date startDate;
    public final Date endDate;
    public final List<String> entryPointUsers;
    public final int requestPerProxy;
    public final boolean extractUsers;
    public final boolean useProxies;

    public final boolean createClusterListener;
    public final boolean createMetricsListener;

    public final int twitterCrawlingSlaveCount;
    public final int proxyCheckingSlaveCount;
    public final int proxyCrawlingSlaveCount;
    public final int graphStoreSlaveCount;

    public final int graphStoreShardCount;
    public final int graphStoreDuplicationLevel;
    public final String graphStoreStoragePath;

    SettingsExtension(@NotNull Config config) throws Exception {
        entryPointUsers =  new ArrayList<>(Arrays.asList(config.getString("rotakka.twittercrawling.entryPointUsers").split(",")));
        requestPerProxy = config.getInt("rotakka.proxycrawling.requestPerProxy");
        extractUsers = config.getBoolean("rotakka.twittercrawling.extractUsers");
        useProxies = config.getBoolean("rotakka.twittercrawling.useProxies");

        try {
            startDate = new SimpleDateFormat("dd-MM-yyyy").parse(config.getString("rotakka.twittercrawling.startDate"));
            endDate = new SimpleDateFormat("dd-MM-yyyy").parse(config.getString("rotakka.twittercrawling.endDate"));
        }
        catch (ParseException e) {
            System.err.println("Failed parsing startDate or endDate from config.");
            throw e;
        }

        createClusterListener = config.getBoolean("rotakka.clusterlistener.create");
        createMetricsListener = config.getBoolean("rotakka.metricslistener.create");

        twitterCrawlingSlaveCount = config.getInt("rotakka.twittercrawling.slaveCount");
        proxyCheckingSlaveCount = config.getInt("rotakka.proxychecking.slaveCount");
        proxyCrawlingSlaveCount = config.getInt("rotakka.proxycrawling.slaveCount");
        graphStoreSlaveCount = config.getInt("rotakka.graphstore.slaveCount");

        graphStoreShardCount = config.getInt("rotakka.graphstore.shardCount");
        graphStoreDuplicationLevel = config.getInt("rotakka.graphstore.duplicationLevel");
        graphStoreStoragePath = config.getString("rotakka.graphstore.storagePath");
    }
}