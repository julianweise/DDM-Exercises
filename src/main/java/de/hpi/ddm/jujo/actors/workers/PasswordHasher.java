package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class PasswordHasher extends AbstractActor {

    public static Props props() {
        return Props.create(PasswordHasher.class);
    }

    private final static int CHUNK_SIZE = 100;

    private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);

    @Data @AllArgsConstructor @SuppressWarnings("unused")
    public static class HashNumbersMessage implements Serializable {
        private static final long serialVersionUID = -7643194361868862395L;
        private HashNumbersMessage() {}
        private int from;
        private int to;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(HashNumbersMessage.class, this::handle)
                .matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(HashNumbersMessage message) {
        String[] hashes = new String[CHUNK_SIZE];
        for(int i = message.from; i <= message.to; ++i) {
            hashes[i % CHUNK_SIZE] = hash(i);
            if (i % CHUNK_SIZE == CHUNK_SIZE - 1) {
                // TODO: Send intermediate result back to parent
            }
        }
    }

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
