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

public class HashDispatcher extends AbstractWorkDispatcher {

    public static Props props(ActorRef master, int[] partnerIds, int prefixes[]) {
        return Props.create(HashDispatcher.class, () -> new HashDispatcher(master, partnerIds, prefixes));
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class HashFoundMessage implements Serializable {
        private static final long serialVersionUID = -6506901694425938486L;
        private int hashInput;
        private String hash;
    }

    private int[] partnerIds;
    private int[] prefixes;
    private String[] hashes;
    private int[] nonces;
    private String[] hashPrefixesToFind = { "11111", "00000" };
    private int nextHashPrefixToFind = 0;

    private HashDispatcher(ActorRef master, int[] partnerIds, int[] prefixes) {
        super(master, HashWorker.props());
        this.partnerIds = partnerIds;
        this.prefixes = prefixes;
        this.hashes = new String[this.partnerIds.length];
        this.nonces = new int[this.partnerIds.length];
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return handleDefaultMessages(receiveBuilder()
                .match(HashFoundMessage.class, this::handle));
    }

    @Override
    protected void initializeWorker(ActorRef worker) {
        // nothing to do
    }

    private void handle(HashFoundMessage message) {
    	int hashPrefix = message.getHash().startsWith("11111") ? 1 : -1;
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

        if (allHashesFound) {
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

        String hashPrefix = this.hashPrefixesToFind[this.nextHashPrefixToFind];

        worker.tell(HashWorker.FindHashMessage.builder()
                .minHashInput(this.partnerIds.length - 1)
                .prefix(hashPrefix)
                .build(),
            this.self()
        );
        this.nextHashPrefixToFind++;
    }

    @Override
    protected boolean hasMoreWork() {
        return this.nextHashPrefixToFind < this.hashPrefixesToFind.length;
    }
}
