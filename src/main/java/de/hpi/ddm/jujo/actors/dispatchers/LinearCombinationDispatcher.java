package de.hpi.ddm.jujo.actors.dispatchers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.Master;
import de.hpi.ddm.jujo.actors.workers.LinearCombinationWorker;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.math.BigInteger;

public class LinearCombinationDispatcher extends AbstractWorkDispatcher {

	private static final int PREFIX_CHUNK_SIZE = 10000;

	public static Props props(ActorRef master, int[] plainPasswords) {
		return Props.create(LinearCombinationDispatcher.class, () -> new LinearCombinationDispatcher(master, plainPasswords));
	}

	@Data @Builder @NoArgsConstructor @AllArgsConstructor
	public static class LinearCombinationFoundMessage implements Serializable {
		private static final long serialVersionUID = -6506901694425938486L;
		private BigInteger prefixes;
	}

	@Data @Builder @NoArgsConstructor
	public static class LinearCombinationNotFoundMessage implements Serializable {
		private static final long serialVersionUID = -3098243399816011764L;
	}

	private int[] plainPasswords;
	private int activeSolvers = 0;
	private BigInteger upperBound;
	private BigInteger lowerBound;
	private int nextBoundToTest = 0;
	private boolean linearCombinationFound = false;

	private LinearCombinationDispatcher(ActorRef master, int[] plainPasswords) {
		super(master, LinearCombinationWorker.props());
		this.plainPasswords = plainPasswords;
		this.initializeBounds();
	}

	private void initializeBounds() {
		byte[] rawInitialPrefixes = new byte[(int) Math.ceil(this.plainPasswords.length / 8.d)]; // 1 bit for each prefix

		for (int i = rawInitialPrefixes.length - 1; i >= 1; --i) {
			rawInitialPrefixes[i] = (byte) 0b10101010;
		}

		if (this.plainPasswords.length == rawInitialPrefixes.length * 8) {
			// bits matched exactly
			rawInitialPrefixes[0] = (byte) 0b10101010;
		} else {
			int bitsLeft = this.plainPasswords.length - (rawInitialPrefixes.length - 1) * 8;
			for (int i = 0; i < bitsLeft; ++i) {
				if (i % 2 != 0) {
					continue;
				}

				rawInitialPrefixes[0] |= 1 << i;
			}
		}

		this.upperBound = new BigInteger(rawInitialPrefixes);
		this.lowerBound = this.upperBound.subtract(BigInteger.valueOf(PREFIX_CHUNK_SIZE));
	}

	@Override
	public AbstractActor.Receive createReceive() {
		return handleDefaultMessages(receiveBuilder()
				.match(LinearCombinationNotFoundMessage.class, this::handle)
				.match(LinearCombinationFoundMessage.class, this::handle));
	}

	@Override
	protected void initializeWorker(ActorRef worker) {
		worker.tell(LinearCombinationWorker.InitializeWorkerMessage.builder()
				.plainPasswords(this.plainPasswords)
				.build(),
			this.self()
		);
	}

	private void handle(LinearCombinationNotFoundMessage message) {
	    this.log().debug("Linear combination not found");
		this.activeSolvers--;
		this.dispatchWork(this.sender());
	}

	@Override
	protected void dispatchWork(ActorRef worker) {
		if (this.linearCombinationFound) {
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
			return;
		}

		this.activeSolvers++;
		if ((this.nextBoundToTest++) % 2 == 0) {
			worker.tell(LinearCombinationWorker.FindLinearCombinationMessage.builder()
							.startPrefixes(this.upperBound)
							.prefixesToTest(PREFIX_CHUNK_SIZE)
							.build(),
					this.self());

			this.upperBound = this.upperBound.add(BigInteger.valueOf(PREFIX_CHUNK_SIZE));
			return;
		}

		worker.tell(LinearCombinationWorker.FindLinearCombinationMessage.builder()
						.startPrefixes(this.lowerBound)
						.prefixesToTest(PREFIX_CHUNK_SIZE)
						.build(),
				this.self());

		this.lowerBound = this.lowerBound.subtract(BigInteger.valueOf(PREFIX_CHUNK_SIZE));
	}

	private void handle(LinearCombinationFoundMessage message) {
		this.activeSolvers--;
        this.log().info("Success! Linear combination found");
		if (!this.linearCombinationFound) {
			this.submitLinearCombination(message.getPrefixes());
		}

		this.linearCombinationFound = true;
		this.dispatchWork(this.sender());
	}

	private void submitLinearCombination(BigInteger prefixes) {
		int[] numericPrefixes = new int[this.plainPasswords.length];
		for (int i = 0; i < numericPrefixes.length; ++i) {
			numericPrefixes[i] = prefixes.testBit(i) ? 1 : -1;
		}

		this.master.tell(Master.LinearCombinationFoundMessage.builder()
				.prefixes(numericPrefixes)
				.build(),
			this.self()
		);
	}

	@Override
	protected boolean hasMoreWork() {
		return !this.linearCombinationFound;
	}
}
