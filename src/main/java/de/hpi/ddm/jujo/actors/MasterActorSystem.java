package de.hpi.ddm.jujo.actors;

import com.typesafe.config.Config;

import akka.actor.ActorSystem;
import akka.cluster.Cluster;


public class MasterActorSystem extends BaseActorSystem {

    public static final String MASTER_ROLE = "master";
    static final int MASTER_PORT = 7879;

    public static void start(String actorSystemName, int workers, String host) {
        final Config config = createConfiguration(actorSystemName, MASTER_ROLE, host, MASTER_PORT, MASTER_PORT);

        final ActorSystem system = createSystem(actorSystemName, config);

        numberOfWorkers = workers;

        Cluster.get(system).registerOnMemberUp(() -> {
            // TODO
        });
    }
}
