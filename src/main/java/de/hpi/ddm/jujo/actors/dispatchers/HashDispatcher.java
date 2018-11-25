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

public class HashDispatcher extends AbstractReapedActor {

    public static Props props(ActorRef master, int[] partnerIds, int prefixes[]) {
        return Props.create(HashDispatcher.class, () -> new HashDispatcher(master, partnerIds, prefixes));
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class HashFoundMessage implements Serializable {
        private static final long serialVersionUID = -6506901694425938486L;
        private int originalPerson;
        private String hash;
    }

    private ActorRef master;
    private int activeSolvers;
    private int[] partnerIds;
    private int[] prefixes;
    private String[] hashes;
    private int nextPersonToHash = 0;

    private HashDispatcher(ActorRef master, int[] partnerIds, int[] prefixes) {
        this.master = master;
        this.partnerIds = partnerIds;
        this.prefixes = prefixes;
        this.hashes = new String[partnerIds.length];
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(DispatcherMessages.AddComputationNodeMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(DispatcherMessages.AddComputationNodeMessage message) {
        ActorRef worker = this.context().actorOf(HashWorker.props().withDeploy(
                new Deploy(new RemoteScope(message.getWorkerAddress())))
        );
        this.context().watch(worker);
        this.dispatchWork(worker);
    }

    private void handle(HashFoundMessage message) {
        this.activeSolvers--;
        this.hashes[message.originalPerson] = message.hash;
        this.dispatchWork(this.sender());

        // TODO check whether all hashes have been generated. If so: Inform master
    }

    private void dispatchWork(ActorRef worker) {
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

    private void handle(Terminated message) {
        this.log().debug(String.format("Watched worker terminated: %s", this.sender()));
        this.master.tell(DispatcherMessages.ReleaseComputationNodeMessage.builder()
                        .workerAddress(this.sender().path().address())
                        .build(),
                this.self()
        );

        if (this.activeSolvers < 1) {
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
    }

}
