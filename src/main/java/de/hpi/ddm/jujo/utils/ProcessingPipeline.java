package de.hpi.ddm.jujo.utils;

import akka.actor.ActorRef;
import akka.actor.Address;
import de.hpi.ddm.jujo.actors.Master;
import de.hpi.ddm.jujo.actors.dispatchers.DispatcherMessages;
import de.hpi.ddm.jujo.actors.dispatchers.GeneDispatcher;
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
        @Builder.Default private TaskState taskState = TaskState.INITIALIZED;
        private ActorRef taskDispatcher;
        private Task nextStep;
        @Builder.Default private Task[] requiredSteps = new Task[0];
        private int[] results;
        @Builder.Default private int numberOfAssignedWorkers = 0;
        @Builder.Default private int maxNumberOfWorkers = Integer.MAX_VALUE;

        public PipelineStep setRequiredStepsConvenience(Task... requiredSteps) {
            this.requiredSteps = requiredSteps;
            return this;
        }

        public void increaseNumberOfAssignedWorkers() {
            this.numberOfAssignedWorkers += 1;
        }
    }

    private enum Task {
        PASSWORD_CRACKING,
        GENE_ANALYSIS,
        LINEAR_COMBINATION,
        HASH_MINING
    }

    private enum TaskState {
        INITIALIZED,
        RUNNING,
        TERMINATED
    }

    private Map<Task, PipelineStep> pipelineSteps = new HashMap<>();

    private List<Address> availableWorkers = new ArrayList<>();
    private Master master;
    private boolean enabled = false;

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

    public void start() {
        this.enabled = true;
        this.assignAvailableWorkers();
    }

    public void addWorker(Address workerAddress) {
        this.availableWorkers.add(workerAddress);
        this.assignAvailableWorkers();
    }

    public void addWorkers(Address workerAddress, int times) {
        for(int i = 0; i < times; i++) {
            this.addWorker(workerAddress);
        }
        this.assignAvailableWorkers();
    }

    public void addWorkers(List<Address> workerAddresses) {
        this.availableWorkers.addAll(workerAddresses);
        this.assignAvailableWorkers();
    }

    public void passwordCrackingFinished(int[] plainPasswords) {
        PipelineStep step = this.pipelineSteps.get(Task.PASSWORD_CRACKING);
        step.setTaskState(TaskState.TERMINATED);
        step.setResults(plainPasswords);

    }

    public void geneAnalysisFinished(int[] bestMatchingPartners) {
        PipelineStep step = this.pipelineSteps.get(Task.GENE_ANALYSIS);
        step.setTaskState(TaskState.TERMINATED);
        step.setResults(bestMatchingPartners);
    }

    private void assignAvailableWorkers() {
        if (!this.enabled) {
            return;
        }
        while (this.availableWorkers.size() > 0) {
            this.assignNextAvailableWorker();
        }
    }

    private void assignNextAvailableWorker() {
        PipelineStep stepToEnhance = null;
        for(PipelineStep step : this.getEnabledSteps()) {
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
            return;
        }
        this.assignWorkerToStep(stepToEnhance);
    }

    private void assignWorkerToStep(PipelineStep step) {
        step.increaseNumberOfAssignedWorkers();
        Address worker = this.availableWorkers.remove(0);
        step.getTaskDispatcher().tell(DispatcherMessages.AddComputationNodeMessage.builder()
                .workerAddress(worker)
                .build(),
            this.master.self()
        );
    }

    private List<PipelineStep> getEnabledSteps() {
        return this.pipelineSteps.values().stream()
                .filter(this::isStepEnabled)
                .collect(Collectors.toList());
    }

    private boolean isStepEnabled(PipelineStep step) {
        if (step.getTaskState() == TaskState.TERMINATED) {
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
