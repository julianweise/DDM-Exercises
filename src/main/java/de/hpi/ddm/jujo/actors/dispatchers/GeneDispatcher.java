package de.hpi.ddm.jujo.actors.dispatchers;

import akka.actor.AbstractActor;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.remote.RemoteScope;
import de.hpi.ddm.jujo.actors.Master;
import de.hpi.ddm.jujo.actors.Reaper;
import de.hpi.ddm.jujo.actors.workers.PasswordWorker;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GeneDispatcher extends AbstractLoggingActor {

	private static final int MAXIMUM_GENE_LIST_SIZE = 126000; // message size in bytes

	public static Props props(ActorRef master, final List<String> geneSequences) {
		return Props.create(GeneDispatcher.class, () -> new GeneDispatcher(master, geneSequences));
	}

	@Data @Builder @NoArgsConstructor @AllArgsConstructor
	public static class BestGenePartnerFoundMessage implements Serializable {
		private static final long serialVersionUID = 2715984585289946783L;
		private int originalPerson;
		private int bestPartner;
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

	private HashMap<Address, Integer> availableResources = new HashMap<>();
	private ActorRef master;
	private List<String> geneSequences;
	private int totalGeneSequencesSize;
	private int[] bestGenePartners;

	private GeneDispatcher(ActorRef master, List<String> geneSequences) {
		this.geneSequences = geneSequences;
		this.master = master;

		this.bestGenePartners = new int[geneSequences.size()];
		for (String geneSequence : geneSequences) {
			totalGeneSequencesSize += geneSequence.length();
		}
	}

	@Override
	public AbstractActor.Receive createReceive() {
		return receiveBuilder()
				.match(DispatcherMessages.AddComputationNodeMessage.class, this::handle)
				.match(BestGenePartnerFoundMessage.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(DispatcherMessages.AddComputationNodeMessage message) {
		this.availableResources.putIfAbsent(message.getNodeAddress(), message.getNumberOfWorkers());

		for (int i = 0; i < message.getNumberOfWorkers(); ++i) {
			// TODO: create gene worker
			// TODO: initialize worker
			// TODO: dispatch work to worker
		}
	}

	private void initializeWorker(ActorRef worker) {
		int sentBytes = 0;
		int nextGeneSequenceIndex = 0;
		int messageSize = 0;
		List<String> geneSequencesInNextMessage = new ArrayList<>();

		while (nextGeneSequenceIndex < this.geneSequences.size()) {
			messageSize = 0;
			geneSequencesInNextMessage.clear();

			for (int i = nextGeneSequenceIndex; i < this.geneSequences.size(); ++i) {
				String geneSequence = this.geneSequences.get(i);

				if (messageSize + geneSequence.length() > MAXIMUM_GENE_LIST_SIZE) {
					break;
				}

				geneSequencesInNextMessage.add(geneSequence);
				messageSize += geneSequence.length();
				nextGeneSequenceIndex++;
			}

			// TODO: send message to worker
		}
	}

	private void dispatchWork(ActorRef worker) {
		// TODO:
	}

	private void handle(BestGenePartnerFoundMessage message) {
		this.bestGenePartners[message.getOriginalPerson()] = message.getBestPartner();
		dispatchWork(this.sender());
	}
}
