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
        private int originalPerson;
        private String hash;
    }

    private int activeSolvers;
    private int[] partnerIds;
    private int[] prefixes;
    private String[] hashes;
    private int nextPersonToHash = 0;
    private int numberOfUnhashedPartners;

    private HashDispatcher(ActorRef master, int[] partnerIds, int[] prefixes) {
        super(master, HashWorker.props());
        this.partnerIds = partnerIds;
        this.prefixes = prefixes;
        this.hashes = new String[partnerIds.length];
        this.numberOfUnhashedPartners = this.partnerIds.length;
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
        this.activeSolvers--;
        this.numberOfUnhashedPartners--;
        this.hashes[message.originalPerson] = message.hash;
        this.dispatchWork(this.sender());

        if (this.numberOfUnhashedPartners < 1) {
            this.submitHashes();
        }
    }

    private void submitHashes() {
        this.log().debug("Submitting generated partner hashes to master.");
        this.master.tell(Master.HashFoundMessage.builder()
                .hashes(this.hashes)
                .build(),
            this.self()
        );
    }

    @Override
    protected void dispatchWork(ActorRef worker) {
        if (this.nextPersonToHash >= this.partnerIds.length) {
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
            return;
        }

        String hashPrefix = this.prefixes[this.nextPersonToHash] > 0 ? "11111" : "00000";

        this.activeSolvers++;
        worker.tell(HashWorker.FindHashMessage.builder()
                .originalPerson(this.nextPersonToHash)
                .partner(this.partnerIds[this.nextPersonToHash])
                .prefix(hashPrefix)
                .build(),
            this.self()
        );
        this.nextPersonToHash++;
    }

    @Override
    protected boolean hasMoreWork() {
        return this.numberOfUnhashedPartners > 0;
    }
}
