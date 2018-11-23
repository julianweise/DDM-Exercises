package de.hpi.ddm.jujo.actors.dispatchers;

import akka.actor.*;
import akka.remote.RemoteScope;
import de.hpi.ddm.jujo.actors.Master;
import de.hpi.ddm.jujo.actors.Reaper;
import de.hpi.ddm.jujo.actors.workers.PasswordWorker;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.*;

public class PasswordDispatcher extends AbstractLoggingActor {

    private static final int LAST_PASSWORD_TO_HASH = 999999;
    private static final int WORK_CHUNK_SIZE = 1000;
    private static final float COMPARATOR_UNDERFLOW_RATIO = 1.5f;

    public static Props props(ActorRef master, final List<String> targetPasswordHashes) {
        return Props.create(PasswordDispatcher.class, () -> new PasswordDispatcher(master, targetPasswordHashes));
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PasswordsHashedMessage implements Serializable {

        private static final long serialVersionUID = 4933006742453684724L;
        private String[] generatedPasswordHashes;
        private int startPassword;
        private int endPassword;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PasswordsCrackedMessage implements Serializable {

        private static final long serialVersionUID = -5853384945199531340L;
        private CrackedPassword[] crackedPasswords;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class CrackedPassword {
        private int plainPassword;
        private String hashedPassword;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();

        // Register at this actor system's reaper
        Reaper.watchWithDefaultReaper(this);
    }

    private HashMap<Address, Integer> availableResources = new HashMap<>();
    private Set<String> uncrackedTargetPasswordHashes;
    private ArrayList<CrackedPassword> crackedPasswords = new ArrayList<>();
    private List<PasswordsHashedMessage> hashesToCompare = new ArrayList<>();
    private ActorRef master;
    private int activeHasher = 0;
    private int activeCompatators = 0;
    private int nextPasswordToHash = 0;

    private PasswordDispatcher(ActorRef master, List<String> targetPasswordHashes) {
        this.uncrackedTargetPasswordHashes = new HashSet<>(targetPasswordHashes);
        for(String targetPasswordHash : targetPasswordHashes) {
            this.crackedPasswords.add(CrackedPassword.builder().hashedPassword(targetPasswordHash).build());
        }
        this.master = master;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(DispatcherMessages.AddComputationNodeMessage.class, this::handle)
                .match(PasswordsHashedMessage.class, this::handle)
                .match(PasswordsCrackedMessage.class, this::handle)
                .matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
                .build();
    }

    private void handle(DispatcherMessages.AddComputationNodeMessage message) {
        this.availableResources.putIfAbsent(message.getNodeAddress(), message.getNumberOfWorkers());

        for (int i = 0; i < message.getNumberOfWorkers(); ++i) {
            ActorRef worker = this.getContext().actorOf(PasswordWorker.props().withDeploy(
                    new Deploy(new RemoteScope(message.getNodeAddress())))
            );

            this.dispatchWork(worker);
        }
    }

    private void handle(PasswordsHashedMessage message) {
        this.hashesToCompare.add(message);
        this.activeHasher--;
        this.dispatchWork(this.sender());
    }

    private void handle(PasswordsCrackedMessage message) {
        for(CrackedPassword crackedPassword : message.crackedPasswords) {
            saveCrackedPassword(crackedPassword);
        }
        this.activeCompatators--;
        this.dispatchWork(this.sender());
    }

    private void saveCrackedPassword(CrackedPassword crackedPassword) {
        for(CrackedPassword storedCrackedPassword : this.crackedPasswords) {
            if (storedCrackedPassword.hashedPassword.equals(crackedPassword.hashedPassword)) {
                storedCrackedPassword.setPlainPassword(crackedPassword.plainPassword);
                this.uncrackedTargetPasswordHashes.remove(storedCrackedPassword.hashedPassword);

                if (!this.hasMoreWork()) {
                    this.submitCrackedPasswords();
                    return;
                }
            }
        }
    }

    private void submitCrackedPasswords() {
        this.master.tell(Master.PasswordsCrackedMessage.builder()
                .plainPasswords(this.crackedPasswords.stream().map(CrackedPassword::getPlainPassword).mapToInt(x -> x).toArray())
                .build(),
            this.self()
        );
    }

    private void dispatchWork(ActorRef worker) {
        if (!this.hasMoreWork()) {
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
            return;
        }

        this.log().info(String.format("Worker requested work. %d active hashers, %d active comparators", this.activeHasher, this.activeCompatators));

        if (this.activeCompatators < 1 && this.hashesToCompare.size() > 0) {
            this.dispatchComparatorWork(worker);
            return;
        }
        if (this.nextPasswordToHash > LAST_PASSWORD_TO_HASH) {
            this.dispatchComparatorWork(worker);
            return;
        }
        if (this.hashesToCompare.size() > this.activeHasher * COMPARATOR_UNDERFLOW_RATIO) {
            this.dispatchComparatorWork(worker);
            return;
        }

        this.dispatchHasherWork(worker);
    }

    private boolean hasMoreWork() {
        return this.uncrackedTargetPasswordHashes.size() > 0;
    }

    private void dispatchComparatorWork(ActorRef comparator) {
        this.log().info("Dispatching comparator work");

        PasswordsHashedMessage workItem = this.hashesToCompare.remove(0);
        comparator.tell(PasswordWorker.ComparePasswordsMessage.builder()
                .targetPasswordHashes(this.uncrackedTargetPasswordHashes)
                .startPassword(workItem.startPassword)
                .endPassword(workItem.endPassword)
                .generatedPasswordHashes(workItem.generatedPasswordHashes)
                .build(),
            this.self()
        );
        this.activeCompatators++;
        this.log().info(String.format("Dispatching comparison work. Currently utilized %d comparators", this.activeCompatators));
    }

    private void dispatchHasherWork(ActorRef hasher) {
        this.log().info("Dispatching hashing work");

        hasher.tell(PasswordWorker.HashPasswordsMessage.builder()
                .startPassword(this.nextPasswordToHash)
                .endPassword(this.nextPasswordToHash + WORK_CHUNK_SIZE - 1)
                .build(),
            this.self()
        );
        this.nextPasswordToHash += WORK_CHUNK_SIZE;
        this.activeHasher++;
        this.log().info(String.format("Dispatching hashing work. Currently utilized %d hashers", this.activeHasher));
    }

}