package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.cluster.Cluster;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class WorkDispatcher extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
    private final Cluster cluster = Cluster.get(this.context().system());
    private ActorRef passwordMaster;

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    private static abstract class BaseWorkMessage implements Serializable {
        private BaseWorkMessage() {}
        private int numberOfWorkers;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class PasswordCrackWorkMessage extends BaseWorkMessage {
        private static final long serialVersionUID = -2936723498723434L;
        private PasswordCrackWorkMessage() {super();}
        private String[] passwordHashes;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(PasswordCrackWorkMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(PasswordCrackWorkMessage message) {
        this.passwordMaster = this.context().system().actorOf(PasswordMaster.props());
        this.passwordMaster.tell(message, this.self());
    }
}
