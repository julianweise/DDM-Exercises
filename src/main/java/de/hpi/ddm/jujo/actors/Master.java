package de.hpi.ddm.jujo.actors;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.typesafe.config.Config;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;


public class Master extends MasterSystem {

    public static final String MASTER_ROLE = "master";

    protected static String getMachineAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
            // TODO: Add logging
        }
    }

    public static void start(String actorSystemName, int workers, String host, int port) {
        final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, port);

        final ActorSystem system = createSystem(actorSystemName, config);

        Cluster.get(system).registerOnMemberUp(() -> {
            // TODO
        });
    }
}
