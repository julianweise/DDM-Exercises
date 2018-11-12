package de.hpi.ddm.jujo.actors;

import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import scala.concurrent.Await;
import scala.concurrent.duration.Duration;

public class MasterSystem {

    protected static Config createConfiguration(String actorSystemName, String actorSystemRole, String host, int port) {

        // Create the Config with fallback to the application config
        return ConfigFactory.parseString(
                "akka.remote.netty.tcp.hostname = \"" + host + "\"\n" +
                        "akka.remote.netty.tcp.port = " + port + "\n" +
                        "akka.remote.artery.canonical.hostname = \"" + host + "\"\n" +
                        "akka.remote.artery.canonical.port = " + port + "\n" +
                        "akka.cluster.roles = [" + actorSystemRole + "]\n" +
                        "akka.cluster.seed-nodes = [\"akka://" + actorSystemName + "@" + host + ":" + port + "\"]")
                .withFallback(ConfigFactory.load("octopus"));
    }

    protected static ActorSystem createSystem(String actorSystemName, Config config) {

        // Create the ActorSystem
        final ActorSystem system = ActorSystem.create(actorSystemName, config);

        // Register a callback that ends the program when the ActorSystem terminates
        system.registerOnTermination(() -> System.exit(0));

        // Register a callback that terminates the ActorSystem when it is detached from the cluster
        Cluster.get(system).registerOnMemberRemoved(() -> {
            system.terminate();

            new Thread(() -> {
                try {
                    Await.ready(system.whenTerminated(), Duration.create(10, TimeUnit.SECONDS));
                } catch (Exception e) {
                    System.exit(-1);
                }
            }).start();
        });

        return system;
    }
}
