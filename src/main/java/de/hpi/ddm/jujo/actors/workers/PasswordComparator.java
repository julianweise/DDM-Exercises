package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class PasswordComparator extends AbstractActor {

    static Props props() {
        return Props.create(PasswordComparator.class);
    }

    private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);

    private String[] passwordHashes;

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    static class CompareHashesMessage implements Serializable {
        private static final long serialVersionUID = -7643124234268862395L;
        private CompareHashesMessage() {}
        private String[] hashes;
        private int startPassword;
        private int endPassword;
    }

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    static class CrackingWorkloadMessage implements Serializable {
        private static final long serialVersionUID = -932847523475928347L;
        private CrackingWorkloadMessage() {}
        private String[] hashes;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CrackingWorkloadMessage.class, this::handle)
                .match(CompareHashesMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(CrackingWorkloadMessage message) {
        this.passwordHashes = message.getHashes();
    }

    private void handle(CompareHashesMessage message) {
        if (this.passwordHashes.length < 1) {
            return;
        }

        for (int i = 0; i < message.hashes.length; ++i) {
            for (String password : this.passwordHashes) {
                if (password.equals(message.hashes[i])) {
                    this.getContext().parent().tell(
                            new PasswordMaster.PasswordCrackedMessage(
                                    password,
                                    message.startPassword + i), this.self()
                    );
                    break;
                }
            }
        }
    }

}
