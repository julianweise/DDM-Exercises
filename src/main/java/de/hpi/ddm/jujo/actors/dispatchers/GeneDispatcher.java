package de.hpi.ddm.jujo.actors.dispatchers;

import akka.actor.AbstractActor;
import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.remote.RemoteScope;
import de.hpi.ddm.jujo.actors.Master;
import de.hpi.ddm.jujo.actors.Reaper;
import de.hpi.ddm.jujo.actors.workers.GeneWorker;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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

	private ActorRef master;
	private List<String> geneSequences;
	private int[] bestGenePartners;
	private int nextOriginalPerson = 0;
	private int activeAnalyzers = 0;

	private GeneDispatcher(ActorRef master, List<String> geneSequences) {
		this.geneSequences = geneSequences;
		this.master = master;
		this.bestGenePartners = new int[geneSequences.size()];
	}

	@Override
	public AbstractActor.Receive createReceive() {
		return receiveBuilder()
				.match(DispatcherMessages.AddComputationNodeMessage.class, this::handle)
				.match(BestGenePartnerFoundMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
				.build();
	}

	private void handle(DispatcherMessages.AddComputationNodeMessage message) {
		ActorRef worker = this.getContext().actorOf(GeneWorker.props().withDeploy(
				new Deploy(new RemoteScope(message.getWorkerAddress())))
		);
		this.context().watch(worker);
		this.initializeWorker(worker);
		this.dispatchWork(worker);
	}

	private void initializeWorker(ActorRef worker) {
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
		--this.activeAnalyzers;
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
				.bestGenePartners(this.bestGenePartners).build(),
			this.self()
		);
	}

	private void dispatchWork(ActorRef worker) {
		if (!this.hasMoreWork()) {
			this.log().info(String.format("Sending poison pill to %s", worker));
			worker.tell(PoisonPill.getInstance(), ActorRef.noSender());
			return;
		}

		worker.tell(GeneWorker.FindBestGenePartnerMessage.builder()
				.originalPerson(this.nextOriginalPerson).build(),
			this.self()
		);
		++this.nextOriginalPerson;
		++this.activeAnalyzers;
	}

	private boolean hasMoreWork() {
		return this.nextOriginalPerson < this.geneSequences.size();
	}

	private void handle(Terminated message) {
		this.log().info(String.format("Watched worker terminated: %s", this.sender()));
		this.master.tell(DispatcherMessages.ReleaseComputationNodeMessage.builder().
						workerAddress(this.sender().path().address()),
				this.self()
		);

		if (this.activeAnalyzers < 1) {
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
	}
}
