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
        private Set<String> targetPasswordHashes;
    }

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(HashPasswordsMessage.class, this::handle)
		        .matchAny(this::handleAny)
                .build();
    }

    private void handle(HashPasswordsMessage message) throws Exception {
        this.log().debug(String.format("Hashing passwords from %d to %d", message.startPassword, message.endPassword));
        String[] hashes = new String[message.endPassword - message.startPassword + 1];
        for (int i = message.startPassword; i <= message.endPassword; ++i) {
            hashes[i - message.startPassword] = AkkaUtils.SHA256(i);
        }
        List<String> targetPasswordsHashes = new ArrayList<>(message.getTargetPasswordHashes());
	    for (int i = 0; i < hashes.length; ++i) {
	    	for (int j = 0; j <targetPasswordsHashes.size(); ++j) {
	    		if (targetPasswordsHashes.get(j).equals(hashes[i])) {
				    this.sender().tell(PasswordDispatcher.PasswordCrackedMessage.builder()
						    .hashedPassword(hashes[i])
						    .plainPassword(i + message.getStartPassword())
						    .build(),
					    this.self()
				    );

				    targetPasswordsHashes.remove(j);
				    break;
			    }
		    }

		    if (targetPasswordsHashes.size() < 1) {
		    	break;
		    }
	    }
	    this.sender().tell(PasswordDispatcher.RequestWorkMessage.builder().build(), this.self());
    }
}
