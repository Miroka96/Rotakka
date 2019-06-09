package de.hpi.rotakka;

/*
 * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
 */

// #fullsample

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import de.hpi.rotakka.actors.twitter.TwitterCrawler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

import java.time.Duration;

public class TwitterCrawlerTest extends JUnitSuite {

    static ActorSystem system;

    @BeforeClass
    public static void setup() {
        system = ActorSystem.create();
    }

    @AfterClass
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void testIt() {
        /*
         * Wrap the whole test procedure within a testkit constructor
         * if you want to receive actor replies or use Within(), etc.
         */
        new TestKit(system) {
            {
                final Props props = TwitterCrawler.props();
                final ActorRef subject = system.actorOf(props);
                final TestKit probe = new TestKit(system);


                within(
                        Duration.ofSeconds(20),
                        () -> {
                            subject.tell(new TwitterCrawler.CrawlUser("elonmusk"), getRef());

                            assert true;
                            // Will wait for the rest of the 3 seconds
                            expectNoMessage();
                            return null;
                        });

            }
        };
    }
}
