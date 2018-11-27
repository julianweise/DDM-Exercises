package de.hpi.ddm.jujo.actors.dispatchers;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.Master;
import de.hpi.ddm.jujo.actors.workers.PasswordWorker;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PasswordDispatcher extends AbstractWorkDispatcher {

    private static final int LAST_PASSWORD_TO_HASH = 999999;
    private static final int WORK_CHUNK_SIZE = 10000;

    public static Props props(ActorRef master, final List<String> targetPasswordHashes) {
        return Props.create(PasswordDispatcher.class, () -> new PasswordDispatcher(master, targetPasswordHashes));
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PasswordCrackedMessage implements Serializable {
        private static final long serialVersionUID = -5853384945199531340L;
        private String hashedPassword;
        private int plainPassword;
    }

    @Data @Builder @NoArgsConstructor
    public static class RequestWorkMessage implements Serializable {
        private static final long serialVersionUID = 8044248503397726954L;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class CrackedPassword {
        private int plainPassword;
        private String hashedPassword;
    }

    private Set<String> uncrackedTargetPasswordHashes;
    private ArrayList<CrackedPassword> crackedPasswords = new ArrayList<>();
    private int nextPasswordToHash = 0;

    private PasswordDispatcher(ActorRef master, List<String> targetPasswordHashes) {
        super(master, PasswordWorker.props());
        this.uncrackedTargetPasswordHashes = new HashSet<>(targetPasswordHashes);
        for(String targetPasswordHash : targetPasswordHashes) {
            this.crackedPasswords.add(CrackedPassword.builder().hashedPassword(targetPasswordHash).build());
        }
    }

    @Override
    public Receive createReceive() {
        return handleDefaultMessages(receiveBuilder()
		        .match(RequestWorkMessage.class, this::handle)
                .match(PasswordCrackedMessage.class, this::handle));
    }

    @Override
    protected void initializeWorker(ActorRef worker) {
        // nothing to do
    }

    private void handle(RequestWorkMessage message) {
    	this.dispatchWork(this.sender());
    }

    private void handle(PasswordCrackedMessage message) {
        this.saveCrackedPassword(CrackedPassword.builder()
	        .hashedPassword(message.getHashedPassword())
	        .plainPassword(message.getPlainPassword())
	        .build()
        );
        this.dispatchWork(this.sender());
    }

    private void saveCrackedPassword(CrackedPassword crackedPassword) {
        for(CrackedPassword storedCrackedPassword : this.crackedPasswords) {
            if (storedCrackedPassword.hashedPassword.equals(crackedPassword.hashedPassword)) {
                storedCrackedPassword.setPlainPassword(crackedPassword.plainPassword);
                this.uncrackedTargetPasswordHashes.remove(storedCrackedPassword.hashedPassword);

                if (this.allPasswordsCracked()) {
                    this.submitCrackedPasswords();
                    return;
                }
            }
        }
    }

    private void submitCrackedPasswords() {
        this.log().debug("Submitting cracked passwords to master.");
        this.master.tell(Master.PasswordsCrackedMessage.builder()
                .plainPasswords(this.crackedPasswords.stream().map(CrackedPassword::getPlainPassword).mapToInt(x -> x).toArray())
                .build(),
            this.self()
        );
    }

    @Override
    protected void dispatchWork(ActorRef worker) {
        if (!this.hasMoreWork()) {
            this.log().debug(String.format("Sending poison pill to %s", worker));
            worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
            return;
        }

	    worker.tell(PasswordWorker.HashPasswordsMessage.builder()
					    .startPassword(this.nextPasswordToHash)
					    .endPassword(this.nextPasswordToHash + WORK_CHUNK_SIZE - 1)
			            .targetPasswordHashes(this.uncrackedTargetPasswordHashes)
					    .build(),
			    this.self()
	    );
	    this.nextPasswordToHash += WORK_CHUNK_SIZE;
    }

	@Override
	protected boolean hasMoreWork() {
        return this.nextPasswordToHash < LAST_PASSWORD_TO_HASH;
    }

    private boolean allPasswordsCracked() {
        return this.uncrackedTargetPasswordHashes.size() < 1;
    }
}
