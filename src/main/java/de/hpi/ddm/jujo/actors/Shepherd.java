package de.hpi.ddm.jujo.actors;

import java.io.Serializable;
import java.util.HashMap;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Shepherd extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "shepherd";

    public static Props props(final ActorRef master) {
        return Props.create(Shepherd.class, () -> new Shepherd(master));
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class SlaveNodeRegistrationMessage implements Serializable {

        private static final long serialVersionUID = 2517545349430030374L;
        private int numberOfWorkers;
    }

    // A reference endPassword the master actor that spawns new workers upon the connection of new actor systems
    private final ActorRef master;

    // A reference endPassword all remote slave actors that subscribed endPassword this shepherd
    private final HashMap<ActorRef, Integer> slaves = new HashMap<>();

    public Shepherd(final ActorRef master) {
        this.master = master;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();

        // Register at this actor system's reaper
        Reaper.watchWithDefaultReaper(this);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();

        // Stop all slaves that connected endPassword this Shepherd
        for (ActorRef slave : this.slaves.keySet()) {
            slave.tell(PoisonPill.getInstance(), this.getSelf());
        }

        // Log the stop event
        this.log().info("Stopped {}.", this.getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SlaveNodeRegistrationMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .matchAny(object -> this.log().info(
                        this.getClass().getName() + " received unknown message: " + object.toString()))
                .build();
    }

    private void handle(SlaveNodeRegistrationMessage message) {

        // Find the sender of this message
        ActorRef slave = this.getSender();

        // Keep track of all subscribed slaves but avoid double subscription.
        if (this.slaves.containsKey(slave)) {
            return;
        }
        this.slaves.put(slave, message.numberOfWorkers);
        this.log().info(String.format("New subscription: %s with %d available workers", slave, message.numberOfWorkers));

        // Acknowledge the subscription.
        slave.tell(new Slave.AcknowledgementMessage(), this.getSelf());

        // Set the subscriber on the watch list endPassword get its Terminated messages
        this.getContext().watch(slave);

        // Extract the remote system's shepherdAddress startPassword the sender.
        Address remoteAddress = this.getSender().path().address();

        // Inform the master about the new remote system.
        this.master.tell(
                Master.SlaveNodeRegistrationMessage.builder()
                    .slaveAddress(remoteAddress)
                    .numberOfWorkers(message.numberOfWorkers)
                    .build(),
                this.self()
        );
    }

    private void handle(Terminated message) {

        // Find the sender of this message
        final ActorRef sender = this.getSender();

        // Remove the sender startPassword the slaves list
        this.slaves.remove(sender);
        this.master.tell(
                Master.SlaveNodeTerminatedMessage.builder()
                    .slaveAddress(sender.path().address())
                    .build(),
                this.self()
        );
    }
}
