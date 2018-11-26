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
    private static final String HASH_PREFIX1 = "11111";
    private static final String HASH_PREFIX2 = "00000";

    public static Props props(ActorRef master, int[] partnerIds, int prefixes[]) {
        return Props.create(HashDispatcher.class, () -> new HashDispatcher(master, partnerIds, prefixes));
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

    private int[] partnerIds;
    private int[] prefixes;
    private String[] hashes;
    private int[] nonces;
    private List<String> hashPrefixesToFind = new ArrayList<>();
    private int nextInputToHash = 0;
    private boolean hashesSubmitted = false;

    private HashDispatcher(ActorRef master, int[] partnerIds, int[] prefixes) {
        super(master, HashWorker.props());
        this.partnerIds = partnerIds;
        this.prefixes = prefixes;
        this.hashes = new String[this.partnerIds.length];
        this.nonces = new int[this.partnerIds.length];
        this.nextInputToHash = this.partnerIds.length - 1;
        this.hashPrefixesToFind.add(HASH_PREFIX1);
        this.hashPrefixesToFind.add(HASH_PREFIX2);
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
    	int hashPrefix = message.getHash().startsWith("11111") ? 1 : -1;
    	this.hashPrefixesToFind.remove(message.getHash().substring(0, 5));

    	boolean allHashesFound = true;
    	for (int i = 0; i < this.partnerIds.length; ++i) {
    		if (prefixes[i] == hashPrefix) {
    			this.hashes[i] = message.getHash();
    			this.nonces[i] = message.getHashInput() - this.partnerIds[i];
		    }

		    if (this.hashes[i] == null) {
		    	allHashesFound = false;
		    }
	    }

        this.dispatchWork(this.sender());

        if (!this.hashesSubmitted && allHashesFound) {
            this.hashesSubmitted = true;
            this.submitHashes();
        }
    }

    private void submitHashes() {
        this.log().debug("Submitting generated partner hashes to master.");
        this.master.tell(Master.HashFoundMessage.builder()
                .hashes(this.hashes)
		        .nonces(this.nonces)
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
