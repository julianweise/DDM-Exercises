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
import sun.misc.Request;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PasswordDispatcher extends AbstractWorkDispatcher {

    private static final int LAST_PASSWORD_TO_HASH = 999999;
    private static final int WORK_CHUNK_SIZE = 10000;
    private static final float COMPARATOR_UNDERFLOW_RATIO = 1.5f;

    private static final String HASH_PREFIX0 = "00000";
    private static final String HASH_PREFIX1 = "11111";

    public static Props props(ActorRef master, final List<String> targetPasswordHashes) {
        return Props.create(PasswordDispatcher.class, () -> new PasswordDispatcher(master, targetPasswordHashes));
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PasswordsHashedMessage implements Serializable {
        private static final long serialVersionUID = 4933006742453684724L;
        private String[] generatedPasswordHashes;
        private int startPassword;
        private int endPassword;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class HashFoundMessage implements Serializable {
        private static final long serialVersionUID = 3465907159184917527L;
        private int hashInput;
        private String hash;
    }

    @Data @Builder @NoArgsConstructor
    public static class RequestWorkMessage implements Serializable {
        private static final long serialVersionUID = 8044248503397726954L;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PasswordsCrackedMessage implements Serializable {
        private static final long serialVersionUID = -5853384945199531340L;
        private CrackedPassword[] crackedPasswords;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class CrackedPassword {
        private int plainPassword;
        private String hashedPassword;
    }

    private Set<String> uncrackedTargetPasswordHashes;
    private ArrayList<CrackedPassword> crackedPasswords = new ArrayList<>();
    private List<PasswordsHashedMessage> hashesToCompare = new ArrayList<>();
    private int activeHashers = 0;
    private int activeCompatators = 0;
    private int nextPasswordToHash = 0;

	private int inputForHashesWithPrefix0 = 0;
	private int inputForHashesWithPrefix1 = 0;
	private boolean hashesSubmitted = false;
	private List<String> hashPrefixesToFind = new ArrayList<>();

    private PasswordDispatcher(ActorRef master, List<String> targetPasswordHashes) {
        super(master, PasswordWorker.props());
        this.uncrackedTargetPasswordHashes = new HashSet<>(targetPasswordHashes);
	    this.hashPrefixesToFind.add(HASH_PREFIX1);
	    this.hashPrefixesToFind.add(HASH_PREFIX0);
        for(String targetPasswordHash : targetPasswordHashes) {
            this.crackedPasswords.add(CrackedPassword.builder().hashedPassword(targetPasswordHash).build());
        }
    }

    @Override
    public Receive createReceive() {
        return handleDefaultMessages(receiveBuilder()
                .match(PasswordsHashedMessage.class, this::handle)
                .match(PasswordsCrackedMessage.class, this::handle)
                .match(HashFoundMessage.class, this::handle)
                .match(RequestWorkMessage.class, this::handle));
    }

    @Override
    protected void initializeWorker(ActorRef worker) {
        // nothing to do
    }

    private void handle(PasswordsHashedMessage message) {
        this.hashesToCompare.add(message);
        this.activeHashers--;
        this.dispatchWork(this.sender());
    }

    private void handle(PasswordsCrackedMessage message) {
        for(CrackedPassword crackedPassword : message.crackedPasswords) {
            saveCrackedPassword(crackedPassword);
        }
        this.activeCompatators--;
        this.dispatchWork(this.sender());
    }

    private void handle(RequestWorkMessage message) {
    	this.dispatchWork(this.sender());
    }

    private void handle(HashFoundMessage message) {
	    if (message.getHash().startsWith(HASH_PREFIX1)) {
		    this.inputForHashesWithPrefix1 = message.getHashInput();
	    } else {
		    this.inputForHashesWithPrefix0 = message.getHashInput();
	    }

	    this.hashPrefixesToFind.remove(message.getHash().substring(0, 5));
	    this.dispatchWork(this.sender());

	    if (!this.hashesSubmitted && this.inputForHashesWithPrefix0 > 0 && this.inputForHashesWithPrefix1 > 0) {
		    this.hashesSubmitted = true;
		    this.submitHashes();
	    }
    }

	private void submitHashes() {
		this.log().debug("Submitting generated partner hashes to master.");
		this.master.tell(Master.HashFoundMessage.builder()
						.inputForHashWithPrefix0(this.inputForHashesWithPrefix0)
						.inputForHashWithPrefix1(this.inputForHashesWithPrefix1)
						.build(),
				this.self()
		);
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

        this.log().debug(String.format("Worker requested work. %d active hashers, %d active comparators", this.activeHashers, this.activeCompatators));

        if (this.activeCompatators < 1 && this.moreHashesToCompare()) {
            this.dispatchComparatorWork(worker);
            return;
        }
        if (!this.morePasswordsToHash() && this.bothHashInputsFound()) {
            this.dispatchComparatorWork(worker);
            return;
        }
        if (!this.moreHashesToCompare()) {
        	this.dispatchHasherWork(worker);
        	return;
        }
        if (this.hashesToCompare.size() > this.activeHashers * COMPARATOR_UNDERFLOW_RATIO) {
            this.dispatchComparatorWork(worker);
            return;
        }

        this.dispatchHasherWork(worker);
    }

	@Override
	protected boolean hasMoreWork() {
    	if (!this.bothHashInputsFound()) {
    		return true;
	    }

        return !this.allPasswordsCracked() && (this.moreHashesToCompare() || this.morePasswordsToHash());
    }

    private boolean morePasswordsToHash() {
        return this.nextPasswordToHash < LAST_PASSWORD_TO_HASH;
    }

    private boolean moreHashesToCompare() {
        return this.hashesToCompare.size() > 0;
    }

    private boolean allPasswordsCracked() {
        return this.uncrackedTargetPasswordHashes.size() < 1;
    }

    private boolean bothHashInputsFound() { return this.inputForHashesWithPrefix0 > 0 && this.inputForHashesWithPrefix1 > 0; }

    private void dispatchComparatorWork(ActorRef comparator) {
        this.log().debug("Dispatching comparator work");

        PasswordsHashedMessage workItem = this.hashesToCompare.remove(0);
        comparator.tell(PasswordWorker.ComparePasswordsMessage.builder()
                .targetPasswordHashes(new HashSet<>(this.uncrackedTargetPasswordHashes))
                .startPassword(workItem.startPassword)
                .endPassword(workItem.endPassword)
                .generatedPasswordHashes(workItem.generatedPasswordHashes)
                .build(),
            this.self()
        );
        this.activeCompatators++;
        this.log().debug(String.format("Dispatching comparison work. Currently utilized %d comparators", this.activeCompatators));
    }

    private void dispatchHasherWork(ActorRef hasher) {
        this.log().debug("Dispatching hashing work");

        hasher.tell(PasswordWorker.HashPasswordsMessage.builder()
                .startPassword(this.nextPasswordToHash)
                .endPassword(this.nextPasswordToHash + WORK_CHUNK_SIZE - 1)
		        .hashPrefixToFind(this.hashPrefixesToFind.toArray(new String[0]))
		        .sendPasswords(this.nextPasswordToHash < LAST_PASSWORD_TO_HASH)
                .build(),
            this.self()
        );
        this.nextPasswordToHash += WORK_CHUNK_SIZE;
        this.activeHashers++;
        this.log().debug(String.format("Dispatching hashing work. Currently utilized %d hashers", this.activeHashers));
    }
}
