package de.hpi.ddm.jujo.actors.dispatchers;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.Master;
import de.hpi.ddm.jujo.actors.workers.GeneWorker;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GeneDispatcher extends AbstractWorkDispatcher {

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

	private List<String> geneSequences;
	private int[] bestGenePartners;
	private int nextOriginalPerson = 0;
	private int activeAnalyzers = 0;

	private GeneDispatcher(ActorRef master, List<String> geneSequences) {
		super(master, GeneWorker.props());
		this.geneSequences = geneSequences;
		this.bestGenePartners = new int[geneSequences.size()];
	}

	@Override
	public AbstractActor.Receive createReceive() {
		return handleDefaultMessages(receiveBuilder()
				.match(BestGenePartnerFoundMessage.class, this::handle));
	}

	@Override
	protected void initializeWorker(ActorRef worker) {
		int nextGeneSequenceIndex = 0;
		int messageSize;
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

			worker.tell(GeneWorker.AddGeneSequencesMessage.builder()
					.geneSequences(geneSequencesInNextMessage.toArray(new String[0])).build(),
				this.self()
			);
		}
	}

	private void handle(BestGenePartnerFoundMessage message) {
		this.activeAnalyzers -= 1;
		this.bestGenePartners[message.getOriginalPerson()] = message.getBestPartner();
		dispatchWork(this.sender());

		if (this.isLastOriginalPerson(message.originalPerson)) {
			this.submitBestGenePartners();
		}
	}

	private boolean isLastOriginalPerson(int originalPerson) {
		return originalPerson >= this.geneSequences.size() - 1;
	}

	private void submitBestGenePartners() {
		this.master.tell(Master.BestGenePartnersFoundMessage.builder()
				.bestGenePartners(this.bestGenePartners)
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

		worker.tell(GeneWorker.FindBestGenePartnerMessage.builder()
				.originalPerson(this.nextOriginalPerson)
				.build(),
			this.self()
		);
		this.nextOriginalPerson += 1;
		this.activeAnalyzers += 1;
	}

	@Override
	protected boolean hasMoreWork() {
		return this.nextOriginalPerson < this.geneSequences.size();
	}
}
