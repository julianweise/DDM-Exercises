package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.Reaper;
import de.hpi.ddm.jujo.actors.dispatchers.LinearCombinationDispatcher;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigInteger;


public class LinearCombinationWorker extends AbstractLoggingActor {

	public static Props props() {
		return Props.create(LinearCombinationWorker.class);
	}

	@Data @Builder @NoArgsConstructor @AllArgsConstructor
	public static final class InitializeWorkerMessage implements Serializable {
		private static final long serialVersionUID = -8941362556606294648L;
		private int[] plainPasswords;
	}

	@Data @Builder @NoArgsConstructor @AllArgsConstructor
	public static final class FindLinearCombinationMessage implements Serializable {
		private static final long serialVersionUID = -8520076050293487823L;
		private BigInteger startPrefixes;
		private int prefixesToTest;
	}

	@Override
	public void preStart() throws Exception {
		super.preStart();

		// Register at this actor system's reaper
		Reaper.watchWithDefaultReaper(this);
	}

	@Override
	public void postStop() throws Exception {
		super.postStop();

		// Log the stop event
		this.log().info("Stopped {}.", this.getSelf());
	}

	@Override
	public AbstractActor.Receive createReceive() {
		return receiveBuilder()
				.match(InitializeWorkerMessage.class, this::handle)
				.match(FindLinearCombinationMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private int[] plainPasswords = new int[0];

	private void handle(InitializeWorkerMessage message) {
		plainPasswords = message.plainPasswords;
	}

	private void handle(FindLinearCombinationMessage message) {
		for (int i = 0; i < message.prefixesToTest; ++i) {
			BigInteger prefixes = message.startPrefixes.add(BigInteger.valueOf(i));
			if (this.sum(prefixes) == 0) {
			    this.foundLinearCombination(prefixes);
				return;
			}
		}
		this.notFoundLinearCombination();
	}

	private int sum(BigInteger prefixes) {
		int sum = 0;
		for (int i = 0; i < this.plainPasswords.length; ++i) {
            sum += this.plainPasswords[i] * (prefixes.testBit(i) ? 1 : -1);
		}
		return sum;
	}

	private void foundLinearCombination(BigInteger prefixes) {
	    this.sender().tell(LinearCombinationDispatcher.LinearCombinationFoundMessage.builder()
                .prefixes(prefixes)
                .build(),
            this.self());
    }

    private void notFoundLinearCombination() {
	    this.sender().tell(LinearCombinationDispatcher.LinearCombinationNotFoundMessage.builder().build(), this.self());
    }
}
