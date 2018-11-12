package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import akka.routing.Router;
import akka.routing.SmallestMailboxRoutingLogic;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PasswordMaster extends AbstractActor {

    private static final int NUMBER_OF_COMPARE_WORKER = 4;

    private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
    private final Cluster cluster = Cluster.get(this.context().system());

    private List<String> passwordHashes = new ArrayList<>();
    private List<Pair<Integer, String>> passwords = new ArrayList<>();

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class CrackingWorkloadMessage implements Serializable {
        private static final long serialVersionUID = -2643239284342862395L;
        private CrackingWorkloadMessage() {}
        @Getter
        private String[] passwordHashes;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class PasswordCrackedMessage implements Serializable {
        private static final long serialVersionUID = -3643239284342862395L;
        private PasswordCrackedMessage() {}
        private String passwordHash;
        private int password;
    }

    @Override
    public void preStart() {
        Cluster.get(this.context().system()).subscribe(this.self(), ClusterEvent.MemberUp.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CrackingWorkloadMessage.class, this::handle)
                .match(PasswordCrackedMessage.class, this::handle)
                .match(PasswordComparer.CompareHashesMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(CrackingWorkloadMessage message) {
        this.passwordHashes.addAll(Arrays.asList(message.passwordHashes));
        // distribute hashing work
    }

    private void handle(PasswordCrackedMessage message) {
        this.passwordHashes.remove(message.passwordHash);
        this.passwords.add(new Pair<>(message.password, message.passwordHash));
        if (this.passwordHashes.size() < 1) {
            // TODO: Send finalized results to Master
        } else {
            // TODO: send update workload to workers
        }
    }

    private void handle(PasswordComparer.CompareHashesMessage message) {
        // distribute comparing work
    }
}
