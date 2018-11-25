package de.hpi.ddm.jujo.actors.dispatchers;

import akka.actor.ActorRef;
import akka.actor.Address;
import akka.actor.Deploy;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.japi.pf.ReceiveBuilder;
import akka.remote.RemoteScope;
import de.hpi.ddm.jujo.actors.AbstractReapedActor;

public abstract class AbstractWorkDispatcher extends AbstractReapedActor {

	protected ActorRef master;
	private Props workerProps;

	protected AbstractWorkDispatcher(ActorRef master, Props workerProps) {
		this.master = master;
		this.workerProps = workerProps;
	}

	protected Receive handleDefaultMessages(ReceiveBuilder receiveBuilder) {
		return receiveBuilder
				.match(DispatcherMessages.AddComputationNodeMessage.class, this::handle)
				.match(Terminated.class, this::handle)
				.matchAny(this::handleAny)
				.build();
	}

	protected void handle(DispatcherMessages.AddComputationNodeMessage message) {
		ActorRef worker = this.getContext().actorOf(this.workerProps.withDeploy(
				new Deploy(new RemoteScope(message.getWorkerAddress())))
		);
		this.context().watch(worker);
		this.initializeWorker(worker);
		this.dispatchWork(worker);
	}

	protected abstract void initializeWorker(ActorRef worker);

	protected abstract void dispatchWork(ActorRef worker);

	protected abstract boolean shouldTerminate();

	protected void handle(Terminated message) {
		this.releaseWorker(this.sender());

		if (this.shouldTerminate()) {
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
		}
	}

	protected void releaseWorker(ActorRef unusedWorker) {
		Address workerAddress = unusedWorker.path().address();
		this.log().debug(String.format("releasing unused worker at %s", workerAddress));
		this.master.tell(DispatcherMessages.ReleaseComputationNodeMessage.builder()
						.workerAddress(workerAddress)
						.build(),
				this.self()
		);
	}
}
