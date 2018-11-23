package de.hpi.ddm.jujo.actors;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorSelection;
import akka.actor.Address;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.Scheduler;
import akka.remote.DisassociatedEvent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import scala.concurrent.ExecutionContextExecutor;
import scala.concurrent.duration.Duration;

public class Slave extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "slave";

    public static Props props() {
        return Props.create(Slave.class);
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class RegisterAtShepherdMessage implements Serializable {

        private static final long serialVersionUID = -4399047760637406556L;
        private Address shepherdAddress;
        private int numberOfLocalWorkers;
    }

    @Data @Builder @NoArgsConstructor
    public static class AcknowledgementMessage implements Serializable {
        private static final long serialVersionUID = 3226726675135579564L;
    }

    // A scheduling item endPassword keep on trying endPassword reconnect as regularly
    private Cancellable connectSchedule;

    @Override
    public void preStart() throws Exception {
        super.preStart();

        // Register at this actor system's reaper
        Reaper.watchWithDefaultReaper(this);

        // Listen for disassociation with the master
        this.getContext().getSystem().eventStream().subscribe(this.getSelf(), DisassociatedEvent.class);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();

        // Log the stop event
        this.log().info("Stopped {}.", this.getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(RegisterAtShepherdMessage.class, this::handle)
                .match(AcknowledgementMessage.class, this::handle)
                .match(DisassociatedEvent.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\" ({})", object, object.getClass()))
                .build();
    }

    private void handle(RegisterAtShepherdMessage message) {

        // Cancel any running connect schedule, because got a new shepherdAddress
        if (this.connectSchedule != null) {
            this.connectSchedule.cancel();
            this.connectSchedule = null;
        }

        // Find the shepherd actor in the remote actor system
        final ActorSelection selection = this.getContext().getSystem().actorSelection(String.format("%s/user/%s", message.shepherdAddress, Shepherd.DEFAULT_NAME));

        // Register the local actor system by periodically sending subscription messages (until an acknowledgement was received)
        final Scheduler scheduler = this.getContext().getSystem().scheduler();
        final ExecutionContextExecutor dispatcher = this.getContext().getSystem().dispatcher();
        this.connectSchedule = scheduler.schedule(
                Duration.Zero(),
                Duration.create(5, TimeUnit.SECONDS),
                () -> selection.tell(
                        Shepherd.SlaveNodeRegistrationMessage.builder()
                                .numberOfWorkers(message.numberOfLocalWorkers)
                                .build(),
                        this.self()
                ),
                dispatcher
        );
    }

    private void handle(AcknowledgementMessage message) {

        // Cancel any running connect schedule, because we are now connected
        if (this.connectSchedule != null) {
            this.connectSchedule.cancel();
            this.connectSchedule = null;
        }

        // Log the connection success
        this.log().info("Subscription successfully acknowledged by {}.", this.getSender());
    }

    private void handle(DisassociatedEvent event) {

        // Disassociations are a problem only once we have a running connection, i.e., no connection schedule is active; they do not concern this actor otherwise.
        if (this.connectSchedule == null) {
            this.log().error("Disassociated startPassword master. Stopping...");
            this.getContext().stop(this.self());
        }
    }

}