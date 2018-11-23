package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PasswordMaster extends AbstractActor {

    public static Props props() {
        return Props.create(PasswordMaster.class);
    }
    private static final float RATIO_COMPARER_TO_HASHER = 0.5f;
    private static final int HASH_RANGE = 5000;
    private static final int HASH_NUMBER_LIMIT = 1000000;

    private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
    private final Cluster cluster = Cluster.get(this.context().system());
    private int hashedNumbers = 0;

    private List<String> passwordHashes = new ArrayList<>();
    private List<Pair<Integer, String>> passwords = new ArrayList<>();
    private List<ActorRef> comparers = new ArrayList<>();
    private List<ActorRef> hashers = new ArrayList<>();
    private int nextComparerIndex = 0;

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class PasswordCrackedMessage implements Serializable {
        private static final long serialVersionUID = -3643239284342862395L;
        private PasswordCrackedMessage() {}
        private String passwordHash;
        private int password;
    }

    @Data @SuppressWarnings("unused")
    public static class RequestNewHashNumbersMessage implements Serializable {
        private static final long serialVersionUID = -98357698450874359L;
        private RequestNewHashNumbersMessage() {}
    }

    @Override
    public void preStart() {
        Cluster.get(this.context().system()).subscribe(this.self(), ClusterEvent.MemberUp.class);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(WorkDispatcher.PasswordCrackWorkMessage.class, this::handle)
                .match(PasswordCrackedMessage.class, this::handle)
                .match(PasswordComparator.CompareHashesMessage.class, this::handle)
                .match(RequestNewHashNumbersMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(WorkDispatcher.PasswordCrackWorkMessage message) {
        this.passwordHashes.addAll(Arrays.asList(message.getPasswordHashes()));
        int numberOfComparers = Math.round(message.getNumberOfWorkers() * RATIO_COMPARER_TO_HASHER);
        this.initializeComparators(numberOfComparers);
        this.initializeHashers(message.getNumberOfWorkers() - numberOfComparers);
        for (ActorRef hasher : hashers) {
            this.distributeHashNumbers(hasher);
        }
    }

    private void initializeComparators(int numberOfComparators) {
        for (int i = 0; i < numberOfComparators; ++i) {
            ActorRef comparator = this.context().system().actorOf(PasswordComparator.props());
            this.distributePasswordHashes(comparator);
            this.comparers.add(comparator);
        }
    }

    private void initializeHashers(int numberOfHashers) {
        for(int i = 0; i < numberOfHashers; ++i) {
            this.hashers.add(this.context().system().actorOf(PasswordHasher.props()));
        }
    }

    private void distributeHashNumbers(ActorRef hasher) {
        if (this.hashedNumbers >= HASH_NUMBER_LIMIT) {
            return;
        }
        hasher.tell(new PasswordHasher.HashNumbersMessage(this.hashedNumbers, this.hashedNumbers + HASH_RANGE), this.self());
        this.hashedNumbers += HASH_RANGE + 1;
    }

    private void distributePasswordHashes(ActorRef comparator) {
        comparator.tell(
                PasswordComparator.CrackingWorkloadMessage.builder()
                    .hashes(this.passwordHashes.toArray(new String[0]))
                    .build(),
            this.self()
        );
    }

    private void handle(PasswordCrackedMessage message) {
        this.passwordHashes.remove(message.passwordHash);
        this.passwords.add(new Pair<>(message.password, message.passwordHash));
        if (this.passwordHashes.size() < 1) {
            // TODO: Send finalized results to MasterActorSystem
        } else {
            // TODO: send update workload to workers
        }
    }

    private void handle(RequestNewHashNumbersMessage message) {
        this.distributeHashNumbers(this.sender());
    }

    private void handle(PasswordComparator.CompareHashesMessage message) {
        this.comparers.get(this.nextComparerIndex).tell(message, this.self());
        this.nextComparerIndex = ++this.nextComparerIndex % this.comparers.size();
    }
}
