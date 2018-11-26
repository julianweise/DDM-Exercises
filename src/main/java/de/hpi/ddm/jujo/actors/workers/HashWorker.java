package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.AbstractReapedActor;
import de.hpi.ddm.jujo.actors.dispatchers.HashDispatcher;
import de.hpi.ddm.jujo.utils.AkkaUtils;
import javafx.util.Pair;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HashWorker extends AbstractReapedActor {

    public static Props props() {
        return Props.create(HashWorker.class);
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static final class FindHashesMessage implements Serializable {
        private static final long serialVersionUID = -1767893664962431821L;
        private int startHashInput;
        private int endHashInput;
        private String[] targetPrefixes;
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(FindHashesMessage.class, this::handle)
		        .matchAny(this::handleAny)
                .build();
    }

    private void handle(FindHashesMessage message) throws Exception {
        List<String> targetHashPrefixes = new ArrayList<>(Arrays.asList(message.getTargetPrefixes()));

        for (int hashInput = message.getStartHashInput(); hashInput < message.getEndHashInput(); ++hashInput) {
            String hash = AkkaUtils.SHA256(hashInput);

            for (int i = 0; i < targetHashPrefixes.size(); ++i) {
                if (hash.startsWith(targetHashPrefixes.get(i))) {
                    this.submitHashFound(hash, hashInput);
                    targetHashPrefixes.remove(i);

                    if (targetHashPrefixes.size() < 1) {
                        return;
                    }
                    break;
                }
            }
        }

        this.sender().tell(HashDispatcher.RequestWorkMessage.builder().build(), this.self());
    }

    private void submitHashFound(String hash, int hashInput) {
        this.sender().tell(HashDispatcher.HashFoundMessage.builder()
                .hash(hash)
                .hashInput(hashInput)
                .build(),
            this.self()
        );
    }
}
