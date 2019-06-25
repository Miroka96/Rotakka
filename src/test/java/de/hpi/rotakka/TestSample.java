package de.hpi.rotakka;

/*
 * Copyright (C) 2009-2019 Lightbend Inc. <https://www.lightbend.com>
 */

// #fullsample

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.javadsl.TestKit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.scalatest.junit.JUnitSuite;

public class TestSample extends JUnitSuite {
    public static final long serialVersionUID = 1;

    public static class SomeActor extends AbstractActor {

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchEquals(
                            "hello",
                            message -> getSender().tell("world", getSelf()))
                    .build();
        }
    }

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
                final Props props = Props.create(SomeActor.class);
                final ActorRef subject = system.actorOf(props);

                subject.tell("hello", getRef());
                expectMsg("world");

            }
        };
    }
}
