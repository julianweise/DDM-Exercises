package de.hpi.ddm.jujo.utils;

import akka.actor.ActorRef;
import akka.actor.Address;
import de.hpi.ddm.jujo.actors.Master;
import de.hpi.ddm.jujo.actors.dispatchers.DispatcherMessages;
import de.hpi.ddm.jujo.actors.dispatchers.GeneDispatcher;
import de.hpi.ddm.jujo.actors.dispatchers.HashDispatcher;
import de.hpi.ddm.jujo.actors.dispatchers.LinearCombinationDispatcher;
import de.hpi.ddm.jujo.actors.dispatchers.PasswordDispatcher;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ProcessingPipeline {

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    private static class PipelineStep {
        private Task task;
        private ActorRef taskDispatcher;
        private Task nextStep;
        private int[] results;
        private long startTimestamp;
        private long endTimestamp;
        @Builder.Default private TaskState taskState = TaskState.INITIALIZED;
        @Builder.Default private Task[] requiredSteps = new Task[0];
        @Builder.Default private int numberOfAssignedWorkers = 0;
        @Builder.Default private int maxNumberOfWorkers = Integer.MAX_VALUE;

        PipelineStep setRequiredStepsConvenience(Task... requiredSteps) {
            this.requiredSteps = requiredSteps;
            return this;
        }

        void increaseNumberOfAssignedWorkers() {
            this.numberOfAssignedWorkers += 1;
        }

        public void decrementNumberOfAssignedWorkers() { this.numberOfAssignedWorkers -= 1; }
    }

    private enum Task {
        NONE,
        PASSWORD_CRACKING,
        GENE_ANALYSIS,
        LINEAR_COMBINATION,
        HASH_MINING
    }

    private enum TaskState {
        INITIALIZED,
        RUNNING,
        ABOUT_TO_TERMINATE,
        TERMINATED
    }

    private Map<Task, PipelineStep> pipelineSteps = new HashMap<>();

    private List<Address> availableWorkers = new ArrayList<>();
    private Master master;
    private boolean enabled = false;
    private long startTimestamp;
    private long endTimestamp;

    public ProcessingPipeline(Master master) {
        this.master = master;
        this.initializePipelineSteps();
    }

    private void initializePipelineSteps() {
        this.pipelineSteps.put(Task.PASSWORD_CRACKING, this.initializePasswordCrackingStep());
        this.pipelineSteps.put(Task.GENE_ANALYSIS, this.initializeGeneAnalysisStep());
    }

    private PipelineStep initializePasswordCrackingStep() {
        List<String> targetPasswordHashes = this.master.getInputDataForColumn(Master.INPUT_DATA_PASSWORD_COLUMN);
        ActorRef passwordDispatcher = this.master.context().actorOf(
                PasswordDispatcher.props(this.master.self(), targetPasswordHashes));
        this.master.getContext().watch(passwordDispatcher);
        return PipelineStep.builder()
                .task(Task.PASSWORD_CRACKING)
                .taskDispatcher(passwordDispatcher)
                .nextStep(Task.LINEAR_COMBINATION)
                .build();
    }

    private PipelineStep initializeGeneAnalysisStep() {
        List<String> geneSequences = this.master.getInputDataForColumn(Master.INPUT_DATA_GENE_COLUMN);
        ActorRef geneDispatcher = this.master.context().actorOf(
                GeneDispatcher.props(this.master.self(), geneSequences));
        this.master.getContext().watch(geneDispatcher);
        return PipelineStep.builder()
                .task(Task.GENE_ANALYSIS)
                .taskDispatcher(geneDispatcher)
                .nextStep(Task.HASH_MINING)
                .maxNumberOfWorkers(geneSequences.size())
                .build();
    }

    private PipelineStep initializeLinearCombinationStep(int[] plainPasswords) {
    	ActorRef linearCombinationDispatcher = this.master.context().actorOf(
			    LinearCombinationDispatcher.props(this.master.self(), plainPasswords));
    	this.master.context().watch(linearCombinationDispatcher);
    	return PipelineStep.builder()
			    .task(Task.LINEAR_COMBINATION)
			    .taskDispatcher(linearCombinationDispatcher)
			    .nextStep(Task.HASH_MINING)
			    .build()
			    .setRequiredStepsConvenience(Task.PASSWORD_CRACKING);
    }

    private PipelineStep initializedHashMiningStep(int[] partnerIds, int[] prefixes) {
        ActorRef hashDispatcher = this.master.context().actorOf(
                HashDispatcher.props(this.master.self(), partnerIds, prefixes));
        this.master.context().watch(hashDispatcher);
        return PipelineStep.builder()
                .task(Task.HASH_MINING)
                .taskDispatcher(hashDispatcher)
                .nextStep(Task.NONE)
                .build()
                .setRequiredStepsConvenience(Task.GENE_ANALYSIS, Task.LINEAR_COMBINATION);
    }

    public void start() {
        this.enabled = true;
        this.startTimestamp = System.currentTimeMillis();
        this.assignAvailableWorkers();
    }

    public void addWorker(Address workerAddress, ActorRef returningWorkDispatcher) {
    	for (PipelineStep step : this.pipelineSteps.values()) {
    		if (step.getTaskDispatcher() != returningWorkDispatcher) {
    			continue;
		    }

		    step.decrementNumberOfAssignedWorkers();
    		this.master.log().info(String.format("%s released 1 worker and has now %d workers", step.getTask(), step.getNumberOfAssignedWorkers()));
		    if (step.getTaskState() == TaskState.RUNNING) {
			    step.setTaskState(TaskState.ABOUT_TO_TERMINATE);
		    }
		    break;
	    }
	    this.addWorker(workerAddress);
    }

    public void addWorkers(Address workerAddress, int times) {
        for(int i = 0; i < times; i++) {
            this.addWorker(workerAddress);
        }
        this.assignAvailableWorkers();
    }

	private void addWorker(Address workerAddress) {
		this.availableWorkers.add(workerAddress);
		this.assignAvailableWorkers();
	}

    public void addWorkers(List<Address> workerAddresses) {
        this.availableWorkers.addAll(workerAddresses);
        this.assignAvailableWorkers();
    }

    public void passwordCrackingFinished(int[] plainPasswords) {
        this.pipelineSteps.get(Task.PASSWORD_CRACKING).setResults(plainPasswords);
        this.finishStep(Task.PASSWORD_CRACKING);

        this.pipelineSteps.put(Task.LINEAR_COMBINATION, this.initializeLinearCombinationStep(plainPasswords));
        this.assignAvailableWorkers();
    }

    public void geneAnalysisFinished(int[] bestMatchingPartners) {
        this.pipelineSteps.get(Task.GENE_ANALYSIS).setResults(bestMatchingPartners);
    	this.finishStep(Task.GENE_ANALYSIS);

        if (this.pipelineSteps.get(Task.LINEAR_COMBINATION).getTaskState() == TaskState.TERMINATED) {
        	this.pipelineSteps.put(Task.HASH_MINING, this.initializedHashMiningStep(
                    this.pipelineSteps.get(Task.GENE_ANALYSIS).getResults(),
                    this.pipelineSteps.get(Task.LINEAR_COMBINATION).getResults())
	        );
	        this.assignAvailableWorkers();
        }
    }

    public void linearCombinationFinished(int[] prefixes) {
        this.pipelineSteps.get(Task.LINEAR_COMBINATION).setResults(prefixes);
        this.finishStep(Task.LINEAR_COMBINATION);

        if (this.pipelineSteps.get(Task.GENE_ANALYSIS).getTaskState() == TaskState.TERMINATED) {
	        this.pipelineSteps.put(Task.HASH_MINING, this.initializedHashMiningStep(
			        this.pipelineSteps.get(Task.GENE_ANALYSIS).getResults(),
			        this.pipelineSteps.get(Task.LINEAR_COMBINATION).getResults())
	        );
	        this.assignAvailableWorkers();
        }
    }

    public void hashMiningFinished(String[] hashes) {
        this.finishStep(Task.HASH_MINING);

        this.endTimestamp = System.currentTimeMillis();

        this.printFinalResults(hashes);
    }

    private void printFinalResults(String[] results) {
        System.out.println(" ================ [FINAL RESULTS] ================");
        for(int i = 0; i < results.length; i++) {
            System.out.printf("%d \t %s\n", i, results[i] );
        }
        System.out.println("\n");
        System.out.printf("[CALCULATION TIME] %d ms \n", this.endTimestamp - this.startTimestamp);
        System.out.println("\n");
        System.out.println(" ================ [FINAL RESULTS] ================");
    }

    private void finishStep(Task stepTask) {
    	PipelineStep step = this.pipelineSteps.get(stepTask);
    	step.setTaskState(TaskState.TERMINATED);
    	step.setEndTimestamp(System.currentTimeMillis());

    	this.master.log().info(String.format("%s finished after %d ms", step.getTask(), step.getEndTimestamp() - step.getStartTimestamp()));
    }

    private void assignAvailableWorkers() {
        if (!this.enabled) {
            return;
        }
        while (this.availableWorkers.size() > 0) {
            if (!this.assignNextAvailableWorker()) {
                break;
            }
        }
    }

    private boolean assignNextAvailableWorker() {
        PipelineStep stepToEnhance = null;
        for(PipelineStep step : this.getStepsWhichNeedMoreWorkers()) {
            if (step.getMaxNumberOfWorkers() <= step.getNumberOfAssignedWorkers()) {
                continue;
            }
            if (stepToEnhance == null) {
                stepToEnhance = step;
                continue;
            }
            if (step.getNumberOfAssignedWorkers() < stepToEnhance.getNumberOfAssignedWorkers()) {
                stepToEnhance = step;
            }
        }
        if (stepToEnhance == null) {
            return false;
        }
        this.assignWorkerToStep(stepToEnhance);
        return true;
    }

    private void assignWorkerToStep(PipelineStep step) {
    	if (step.getTaskState() == TaskState.INITIALIZED) {
    		step.setStartTimestamp(System.currentTimeMillis());
    		step.setTaskState(TaskState.RUNNING);
    		this.master.log().info(String.format("%s started", step.getTask()));
	    }

        step.increaseNumberOfAssignedWorkers();
        Address worker = this.availableWorkers.remove(0);
        step.getTaskDispatcher().tell(DispatcherMessages.AddComputationNodeMessage.builder()
                .workerAddress(worker)
                .build(),
            this.master.self()
        );
        this.master.log().info(String.format("%s has now %d workers", step.getTask(), step.getNumberOfAssignedWorkers()));
    }

    private List<PipelineStep> getStepsWhichNeedMoreWorkers() {
        return this.pipelineSteps.values().stream()
                .filter(this::needsMoreWorkers)
                .collect(Collectors.toList());
    }

    private boolean needsMoreWorkers(PipelineStep step) {
        if (step.getTaskState() == TaskState.TERMINATED) {
            return false;
        }
	    if (step.getTaskState() == TaskState.ABOUT_TO_TERMINATE) {
		    return false;
	    }
        for (Task requiredStep : step.getRequiredSteps()) {
            if (this.pipelineSteps.get(requiredStep).getTaskState() != TaskState.TERMINATED) {
                return false;
            }
        }
        return true;
    }

    public boolean hasFinished() {
        for (PipelineStep step : this.pipelineSteps.values()) {
            if (step.getTaskState() != TaskState.TERMINATED) {
                return false;
            }
        }
        return true;
    }
}
