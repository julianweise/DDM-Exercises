package de.hpi.ddm.jujo.actors.dispatchers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.Master;
import de.hpi.ddm.jujo.actors.workers.HashWorker;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class HashDispatcher extends AbstractWorkDispatcher {

    private static final int HASH_CHUNK_SIZE = 10000;
    private static final String HASH_PREFIX0 = "00000";
    private static final String HASH_PREFIX1 = "11111";

    public static Props props(ActorRef master, int numberOfPersons) {
        return Props.create(HashDispatcher.class, () -> new HashDispatcher(master, numberOfPersons));
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class HashFoundMessage implements Serializable {
        private static final long serialVersionUID = -6506901694425938486L;
        private int hashInput;
        private String hash;
    }

    @Data @Builder @NoArgsConstructor
    public static class RequestWorkMessage implements Serializable {
        private static final long serialVersionUID = -7147051060944662922L;
    }

    private int[] hashInputs = new int[2];
    private List<String> hashPrefixesToFind = new ArrayList<>();
    private int nextInputToHash = 0;
    private boolean hashesSubmitted = false;

    private HashDispatcher(ActorRef master, int numberOfPersons) {
        super(master, HashWorker.props());
        this.nextInputToHash = numberOfPersons - 1;
        this.hashPrefixesToFind.add(HASH_PREFIX1);
        this.hashPrefixesToFind.add(HASH_PREFIX0);
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return handleDefaultMessages(receiveBuilder()
                .match(RequestWorkMessage.class, this::handle)
                .match(HashFoundMessage.class, this::handle));
    }

    @Override
    protected void initializeWorker(ActorRef worker) {
        // nothing to do
    }

    private void handle(RequestWorkMessage message) {
        this.dispatchWork(this.sender());
    }

    private void handle(HashFoundMessage message) {
        if (message.getHash().startsWith(HASH_PREFIX0)) {
            this.hashInputs[0] = message.getHashInput();
            this.hashPrefixesToFind.remove(HASH_PREFIX0);
        } else {
            this.hashInputs[1] = message.getHashInput();
            this.hashPrefixesToFind.remove(HASH_PREFIX1);
        }
        this.dispatchWork(this.sender());

        if (!this.hashesSubmitted && this.allHashesFound()) {
            this.submitHashes();
        }
    }

    private boolean allHashesFound() {
        for (int hashInput : this.hashInputs) {
            if (hashInput <= 0) {
                return false;
            }
        }

        return true;
    }

    private void submitHashes() {
	    this.log().debug("Submitting generated partner hashes to master.");
	    this.hashesSubmitted = true;
	    this.master.tell(Master.HashFoundMessage.builder()
			    .hashInputs(this.hashInputs)
                .build(),
            this.self()
        );
    }

    @Override
    protected void dispatchWork(ActorRef worker) {
        if (!this.hasMoreWork()) {
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
            return;
        }

        worker.tell(HashWorker.FindHashesMessage.builder()
                .startHashInput(this.nextInputToHash)
                .endHashInput(this.nextInputToHash + HASH_CHUNK_SIZE)
                .targetPrefixes(this.hashPrefixesToFind.toArray(new String[0]))
                .build(),
            this.self()
        );
        this.nextInputToHash += HASH_CHUNK_SIZE;
    }

    @Override
    protected boolean hasMoreWork() {
        return this.hashPrefixesToFind.size() > 0;
    }
}
