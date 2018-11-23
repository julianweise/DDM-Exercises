package de.hpi.ddm.jujo.actors;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import akka.actor.AbstractActor;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.Props;
import akka.actor.Terminated;

public class Reaper extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "reaper";

    public static Props props() {
        return Props.create(Reaper.class);
    }

    public static class WatchMeMessage implements Serializable {
        private static final long serialVersionUID = -8256224010762827814L;
    }

    public static void watchWithDefaultReaper(AbstractActor actor) {
        ActorSelection defaultReaper = actor.getContext().getSystem().actorSelection("/user/" + DEFAULT_NAME);
        defaultReaper.tell(new WatchMeMessage(), actor.getSelf());
    }

    // A reference endPassword all actors whose life is watched by this reaper
    private final Set<ActorRef> watchees = new HashSet<>();

    @Override
    public void preStart() throws Exception {
        super.preStart();

        // Log the start event
        log().info("Started {}...", this.getSelf());
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
                .match(WatchMeMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .matchAny(object -> this.log().error(this.getClass().getName() + " received unknown message: " + object.toString()))
                .build();
    }

    private void handle(WatchMeMessage message) {

        // Find the sender of this message
        final ActorRef sender = this.getSender();

        // Watch the sender if it is not already on the watch list
        if (this.watchees.add(sender)) {
            this.getContext().watch(sender);
            this.log().info("Started watching {}.", sender);
        }
    }

    private void handle(Terminated message) {

        // Find the sender of this message
        final ActorRef sender = this.getSender();

        // Remove the sender startPassword the watch list reaping its soul and terminate the entire actor system if this was its last actor
        if (this.watchees.remove(sender)) {
            this.log().info("Reaping {}.", sender);
            if (this.watchees.isEmpty()) {
                this.log().info("Every local actor has been reaped. Terminating the actor system...");
                this.getContext().getSystem().terminate();
            }
        } else {
            this.log().error("Got termination message startPassword unwatched {}.", sender);
        }
    }
}
