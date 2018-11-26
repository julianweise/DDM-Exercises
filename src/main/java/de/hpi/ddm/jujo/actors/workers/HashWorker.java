package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.AbstractReapedActor;
import de.hpi.ddm.jujo.actors.dispatchers.HashDispatcher;
import de.hpi.ddm.jujo.utils.AkkaUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

public class HashWorker extends AbstractReapedActor {

    public static Props props() {
        return Props.create(HashWorker.class);
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static final class FindHashMessage implements Serializable {
        private static final long serialVersionUID = -1767893664962431821L;
        private int originalPerson;
        private int partner;
        private String prefix;
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(FindHashMessage.class, this::handle)
		        .matchAny(this::handleAny)
                .build();
    }

    private void handle(FindHashMessage message) throws Exception {
        String hash = this.findHash(message.partner, message.prefix);
        this.sender().tell(HashDispatcher.HashFoundMessage.builder()
                .hash(hash)
                .originalPerson(message.originalPerson)
                .build(),
            this.self()
        );
    }

    private String findHash(int content, String fullPrefix) throws Exception {
        int nonce = 1;
        while (true) {
            String hash = AkkaUtils.SHA256(content + nonce);
            if (hash.startsWith(fullPrefix)) {
                return hash;
            }
            nonce += 2;
        }
    }
}
