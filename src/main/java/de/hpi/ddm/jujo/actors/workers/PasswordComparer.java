package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class PasswordComparer extends AbstractActor {

    private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);

    private String[] passwordHashes;

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class CompareHashesMessage implements Serializable {
        private static final long serialVersionUID = -7643124234268862395L;
        private CompareHashesMessage() {}
        private String[] hashes;
        private int startPassword;
        private int endPassword;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(PasswordMaster.CrackingWorkloadMessage.class, this::handle)
                .match(CompareHashesMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(PasswordMaster.CrackingWorkloadMessage message) {
        this.passwordHashes = message.getPasswordHashes();
    }

    private void handle(CompareHashesMessage message) {
        if (this.passwordHashes.length < 1) {
            return;
        }

        for (int i = 0; i < message.hashes.length; ++i) {
            for (String password : this.passwordHashes) {
                if (password.equals(message.hashes[i])) {
                    // TODO send message to parent
                    break;
                }
            }
        }
    }

}
