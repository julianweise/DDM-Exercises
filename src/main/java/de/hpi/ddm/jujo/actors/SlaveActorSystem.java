package de.hpi.ddm.jujo.actors;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
import com.typesafe.config.Config;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class SlaveActorSystem extends BaseActorSystem {

    public static final String SLAVE_ROLE = "slave";
    private static final int SLAVE_PORT = 7877;

    protected static String getMachineAddress() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "localhost";
            // TODO: Add logging
        }
    }

    public static void start(String actorSystemName, int workers, String host) {
        final Config config = createConfiguration(actorSystemName, SLAVE_ROLE, host, MasterActorSystem.MASTER_PORT, SLAVE_PORT);

        final ActorSystem system = createSystem(actorSystemName, config);

        numberOfWorkers = workers;

        Cluster.get(system).registerOnMemberUp(() -> {
            // TODO
        });
    }
}
