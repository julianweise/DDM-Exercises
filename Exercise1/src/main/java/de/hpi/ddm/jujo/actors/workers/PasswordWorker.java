package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.AbstractReapedActor;
import de.hpi.ddm.jujo.actors.dispatchers.PasswordDispatcher;
import de.hpi.ddm.jujo.utils.AkkaUtils;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PasswordWorker extends AbstractReapedActor {

    public static Props props() {
        return Props.create(PasswordWorker.class);
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class HashPasswordsMessage implements Serializable {
        private static final long serialVersionUID = 7209760767255490488L;
        private int startPassword;
        private int endPassword;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class ComparePasswordsMessage implements Serializable {
        private static final long serialVersionUID = -8003373471127707382L;
        private Set<String> targetPasswordHashes;
        private String[] generatedPasswordHashes;
        private int startPassword;
        private int endPassword;
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(HashPasswordsMessage.class, this::handle)
                .match(ComparePasswordsMessage.class, this::handle)
		        .matchAny(this::handleAny)
                .build();
    }

    private void handle(HashPasswordsMessage message) throws Exception {
        this.log().debug(String.format("Hashing passwords from %d to %d", message.startPassword, message.endPassword));
        String[] hashes = new String[message.endPassword - message.startPassword + 1];
        for (int i = message.startPassword; i <= message.endPassword; ++i) {
            hashes[i - message.startPassword] = AkkaUtils.SHA256(i);
        }
        this.sendHashes(hashes, message.startPassword, message.endPassword);
    }

    private void sendHashes(String[] hashes, int startPassword, int endPassword) {
        this.context().parent().tell(
                PasswordDispatcher.PasswordsHashedMessage.builder()
                        .generatedPasswordHashes(hashes)
                        .startPassword(startPassword)
                        .endPassword(endPassword)
                        .build(),
                this.self()
        );
    }

    private void handle(ComparePasswordsMessage message) {
        this.log().debug(String.format("Comparing %d password hashes against %d target hashes", message.generatedPasswordHashes.length, message.targetPasswordHashes.size()));
        ArrayList<PasswordDispatcher.CrackedPassword> crackedPasswords = new ArrayList<>();
        for (int i = 0; i < message.generatedPasswordHashes.length; ++i) {
            for (String targetPasswordHash : message.targetPasswordHashes) {
                if (targetPasswordHash.equals(message.getGeneratedPasswordHashes()[i])) {
                    crackedPasswords.add(PasswordDispatcher.CrackedPassword.builder()
                        .hashedPassword(targetPasswordHash)
                        .plainPassword(i + message.startPassword)
                        .build()
                    );
                    break;
                }
            }

            if (crackedPasswords.size() >= message.targetPasswordHashes.size()) {
                // found all passwords for this message
                break;
            }
        }
        this.sendCrackedPasswords(crackedPasswords);
    }

    private void sendCrackedPasswords(List<PasswordDispatcher.CrackedPassword> crackedPasswords) {
        this.sender().tell(PasswordDispatcher.PasswordsCrackedMessage.builder()
                .crackedPasswords(crackedPasswords.toArray(new PasswordDispatcher.CrackedPassword[0]))
                .build(),
            this.self()
        );
    }
}
