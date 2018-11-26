package de.hpi.ddm.jujo.utils;

import akka.actor.ActorRef;
import akka.actor.Address;
import de.hpi.ddm.jujo.actors.Master;
import de.hpi.ddm.jujo.actors.dispatchers.DispatcherMessages;
import de.hpi.ddm.jujo.actors.dispatchers.GeneDispatcher;
import de.hpi.ddm.jujo.actors.dispatchers.LinearCombinationDispatcher;
import de.hpi.ddm.jujo.actors.dispatchers.PasswordDispatcher;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.Arrays;
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
        private long startTimestamp;
	    private long endTimestamp;
	    @Builder.Default private int[] results = new int[0];
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

        void decrementNumberOfAssignedWorkers() { this.numberOfAssignedWorkers -= 1; }
    }

    private enum Task {
        PASSWORD_CRACKING,
        GENE_ANALYSIS,
        LINEAR_COMBINATION,
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
    private int inputForHashesWithPrefix0 = 0;
    private int inputForHashesWithPrefix1 = 0;

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
			    .maxNumberOfWorkers(4)
			    .build()
			    .setRequiredStepsConvenience(Task.PASSWORD_CRACKING);
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
    }

	private void addWorker(Address workerAddress) {
		this.availableWorkers.add(workerAddress);
		this.assignAvailableWorkers();
	}

    public void passwordCrackingFinished(int[] plainPasswords) {
    	if (this.inputForHashesWithPrefix0 > 0 && this.inputForHashesWithPrefix1 > 0) {
    		this.finishStep(Task.PASSWORD_CRACKING, plainPasswords);
	    } else {
    		this.pipelineSteps.get(Task.PASSWORD_CRACKING).setResults(plainPasswords);
	    }

        this.pipelineSteps.put(Task.LINEAR_COMBINATION, this.initializeLinearCombinationStep(plainPasswords));
        this.assignAvailableWorkers();
    }

    public void geneAnalysisFinished(int[] bestMatchingPartners) {
    	this.finishStep(Task.GENE_ANALYSIS, bestMatchingPartners);
    }

    public void linearCombinationFinished(int[] prefixes) {
        this.finishStep(Task.LINEAR_COMBINATION, prefixes);
    }

    public void hashMiningFinished(int inputForHashesWithPrefix0, int inputForHashesWithPrefix1) {
    	this.inputForHashesWithPrefix0 = inputForHashesWithPrefix0;
    	this.inputForHashesWithPrefix1 = inputForHashesWithPrefix1;

    	if (this.pipelineSteps.get(Task.PASSWORD_CRACKING).getResults().length > 0) {
    		this.finishStep(Task.PASSWORD_CRACKING, this.pipelineSteps.get(Task.PASSWORD_CRACKING).getResults());
	    }
        this.tryFinishPipeline();
    }

	private void finishStep(Task stepTask, int[] results) {
		PipelineStep step = this.pipelineSteps.get(stepTask);
		step.setResults(results);
		step.setTaskState(TaskState.TERMINATED);
		step.setEndTimestamp(System.currentTimeMillis());

		this.master.log().info(String.format("%s finished after %d ms", step.getTask(), step.getEndTimestamp() - step.getStartTimestamp()));
		this.tryFinishPipeline();
    }

	private void tryFinishPipeline() {
    	if (this.pipelineSteps.size() < Task.values().length) {
    		return;
	    }

		for (PipelineStep pipelineStep : this.pipelineSteps.values()) {
			if (pipelineStep.getTaskState() != TaskState.TERMINATED) {
				return;
			}
		}

		this.printFinalResults();
	}

    private void printFinalResults() {
	    this.endTimestamp = System.currentTimeMillis();
    	List<String> names = this.master.getInputDataForColumn(Master.INPUT_DATA_NAME_COLUMN);
    	int[] passwords = this.pipelineSteps.get(Task.PASSWORD_CRACKING).getResults();
    	int[] linearPrefixes = this.pipelineSteps.get(Task.LINEAR_COMBINATION).getResults();
    	int[] matchingGenePartners = this.pipelineSteps.get(Task.GENE_ANALYSIS).getResults();
    	int[] hashNonces = new int[passwords.length];
    	String[] hashes = new String[passwords.length];

	    String hash0;
	    String hash1;
    	try {
		    hash0 = AkkaUtils.SHA256(this.inputForHashesWithPrefix0);
		    hash1 = AkkaUtils.SHA256(this.inputForHashesWithPrefix1);
	    } catch (Exception exception) {
    		this.master.log().error(exception, "Can not calculate finale results");
    		return;
	    }

    	for (int i = 0; i < passwords.length; ++i) {
    		hashNonces[i] = linearPrefixes[i] == 1
				    ? this.inputForHashesWithPrefix1 - matchingGenePartners[i]
				    : this.inputForHashesWithPrefix0 - matchingGenePartners[i];
    		hashes[i] = linearPrefixes[i] == 1
				    ? hash1
				    : hash0;
	    }

        System.out.println("\n\n\n");
        System.out.println(" ========================== [FINAL RESULTS] ==========================");
        this.printFinalResultsTableHeader();
        for(int i = 0; i < hashes.length; i++) {
            this.printFinalResultsRow(
            		i,
		            names.get(i),
		            passwords[i],
		            linearPrefixes[i],
		            names.get(matchingGenePartners[i]),
		            hashNonces[i],
		            hashes[i]
            );
        }
        System.out.println(" ========================== [FINAL RESULTS] ==========================");
        System.out.println("\n\n");
        System.out.println(" ======================= [PERFORMANCE RESULTS] =======================");
        for (PipelineStep step : this.pipelineSteps.values()) {
        	System.out.printf(" %s: %d ms \n", step.getTask(), step.getEndTimestamp() - step.getStartTimestamp());
        }

        System.out.println("\n");
	    System.out.printf(" Total: %d ms \n", this.endTimestamp - this.startTimestamp);
        System.out.println(" ======================= [PERFORMANCE RESULTS] =======================");
        System.out.println("\n\n\n");
    }

	private static final String OUTPUT_ID_COLUMN                    = "ID          ";
    private static final String OUTPUT_NAME_COLUMN                  = "Name        ";
    private static final String OUTPUT_PASSWORD_COLUMN              = "Password    ";
    private static final String OUTPUT_PASSWORD_PREFIX_COLUMN       = "Prefix      ";
    private static final String OUTPUT_BEST_GENE_PARTNER_COLUMN     = "Partner     ";
    private static final String OUTPUT_FINAL_HASH_NONCE_COLUMN      = "Hash-Nonce  ";
    private static final String OUTPUT_FINAL_HASH_COLUMN            = "Hash        ";
    private static final int OUTPUT_COLUMN_LENGTH = 12;

    private void printFinalResultsTableHeader() {
    	String columns = String.format(" %s| %s| %s| %s| %s| %s| %s",
			    OUTPUT_ID_COLUMN,
			    OUTPUT_NAME_COLUMN,
			    OUTPUT_PASSWORD_COLUMN,
			    OUTPUT_PASSWORD_PREFIX_COLUMN,
			    OUTPUT_BEST_GENE_PARTNER_COLUMN,
			    OUTPUT_FINAL_HASH_NONCE_COLUMN,
			    OUTPUT_FINAL_HASH_COLUMN);
    	char[] line = new char[columns.length()];
	    Arrays.fill(line, '-');

	    System.out.printf("%s\n%s\n", columns, new String(line));
    }

    private void printFinalResultsRow(int id, String name, int password, int passwordPrefix, String genePartner, int hashNonce, String hash) {
	    System.out.printf(" %s| %s| %s| %s| %s| %s| %s\n",
			    this.padRight(id),
			    this.padRight(name),
			    this.padRight(password),
			    this.padRight(passwordPrefix),
			    this.padRight(genePartner),
			    this.padRight(hashNonce),
			    this.padRight(hash));
    }

    private String padRight(Object input) {
    	return String.format("%1$-" + OUTPUT_COLUMN_LENGTH + "s", input);
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
        this.assignWorkerToStep(stepToEnhance, this.availableWorkers.remove(0));
        return true;
    }

    private void assignWorkerToStep(PipelineStep step, Address workerAddress) {
	    if (step.getTaskState() == TaskState.INITIALIZED) {
		    step.setStartTimestamp(System.currentTimeMillis());
		    step.setTaskState(TaskState.RUNNING);
		    this.master.log().info(String.format("%s started", step.getTask()));
	    }

	    step.increaseNumberOfAssignedWorkers();
	    step.getTaskDispatcher().tell(DispatcherMessages.AddComputationNodeMessage.builder()
					    .workerAddress(workerAddress)
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
