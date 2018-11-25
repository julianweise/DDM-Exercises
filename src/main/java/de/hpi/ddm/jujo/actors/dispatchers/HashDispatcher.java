package de.hpi.ddm.jujo.actors.dispatchers;

import akka.actor.AbstractActor;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Deploy;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.remote.RemoteScope;
import de.hpi.ddm.jujo.actors.AbstractReapedActor;
import de.hpi.ddm.jujo.actors.Reaper;
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

    private HashDispatcher(ActorRef master, int[] partnerIds, int[] prefixes) {
        super(master, HashWorker.props());
        this.partnerIds = partnerIds;
        this.prefixes = prefixes;
        this.hashes = new String[partnerIds.length];
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return handleDefaultMessages(receiveBuilder());
    }

    @Override
    protected void initializeWorker(ActorRef worker) {
        // nothing to do
    }

    private void handle(HashFoundMessage message) {
        this.activeSolvers--;
        this.hashes[message.originalPerson] = message.hash;
        this.dispatchWork(this.sender());

        // TODO check whether all hashes have been generated. If so: Inform master
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
    protected boolean shouldTerminate() {
        return this.activeSolvers < 1;
    }

}
