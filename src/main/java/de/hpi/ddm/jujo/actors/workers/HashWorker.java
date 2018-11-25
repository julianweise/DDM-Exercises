package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.AbstractReapedActor;
import de.hpi.ddm.jujo.actors.dispatchers.HashDispatcher;
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

    private void handle(FindHashMessage message) {
        String hash = this.findHash(message.partner, message.prefix);
        this.sender().tell(HashDispatcher.HashFoundMessage.builder()
                .hash(hash)
                .originalPerson(message.originalPerson)
                .build(),
            this.self()
        );
    }

    private String findHash(int content, String fullPrefix) {
        int nonce = 0;
        while (true) {
            String hash = this.hash(content + nonce++);
            if (hash.startsWith(fullPrefix)) {
                return hash;
            }
        }
    }

    @SuppressWarnings("Duplicates")
    private String hash(int number) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hashedBytes = digest.digest(String.valueOf(number).getBytes(StandardCharsets.UTF_8));

            StringBuilder stringBuffer = new StringBuilder();
            for (byte hashedByte : hashedBytes) {
                stringBuffer.append(Integer.toString((hashedByte & 0xff) + 0x100, 16).substring(1));
            }
            return stringBuffer.toString();
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
