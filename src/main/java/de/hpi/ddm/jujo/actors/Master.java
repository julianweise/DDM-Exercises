package de.hpi.ddm.jujo.actors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;

import akka.actor.*;
import de.hpi.ddm.jujo.actors.dispatchers.DispatcherMessages;
import de.hpi.ddm.jujo.actors.dispatchers.GeneDispatcher;
import de.hpi.ddm.jujo.actors.dispatchers.PasswordDispatcher;
import de.hpi.ddm.jujo.utils.ProcessingPipeline;
import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "master";
    public static final String INPUT_DATA_PASSWORD_COLUMN = "Password";
    public static final String INPUT_DATA_GENE_COLUMN = "Gene";

    public static Props props(final int numLocalWorkers, final int minNumberOfSlaves, final String pathToInputFile) {
        return Props.create(Master.class, () -> new Master(numLocalWorkers, minNumberOfSlaves, pathToInputFile));
    }


    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class SlaveNodeRegistrationMessage implements Serializable {

        private static final long serialVersionUID = -1682543505601299772L;
        private Address slaveAddress;
        private int numberOfWorkers;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class SlaveNodeTerminatedMessage implements Serializable {

        private static final long serialVersionUID = -3053321777422537935L;
        private Address slaveAddress;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class PasswordsCrackedMessage implements Serializable {

        private static final long serialVersionUID = 4075169360742985046L;
        private int[] plainPasswords;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class BestGenePartnersFoundMessage implements Serializable {
        private static final long serialVersionUID = -9200570697342104107L;

        private int[] bestGenePartners;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class LinearCombinationFoundMessage implements  Serializable {
	    private static final long serialVersionUID = -645751953498374126L;
	    private int[] prefixes;
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        Reaper.watchWithDefaultReaper(this);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        this.log().info("Stopped {}.", this.getSelf());
    }

    private int currentNumberOfSlaves = 0;
    private int minNumberOfSlavesToStartWork;
    private Map<String, List<String>> inputData = new HashMap<>();
    private ProcessingPipeline pipeline;

    public Master(int numLocalWorkers, int minNumberOfSlavesToStartWork, String pathToInputFile) {
        try {
            this.parseInputFile(pathToInputFile);
        } catch (IOException e) {
            this.log().error(e, "Error while processing input file.");
        }
        this.pipeline =  new ProcessingPipeline(this);
        this.minNumberOfSlavesToStartWork = minNumberOfSlavesToStartWork + 1; // local actor system counts as one slave
        this.self().tell(SlaveNodeRegistrationMessage.builder()
                .slaveAddress(this.self().path().address())
                .numberOfWorkers(numLocalWorkers)
                .build(),
                this.self()
        );
    }

    public List<String> getInputDataForColumn(String columnName) {
        return this.inputData.get(columnName);
    }

    private void parseInputFile(String pathToInputFile) throws IOException {
        File file = new File(pathToInputFile);
        CsvReader csvReader = new CsvReader();
        csvReader.setContainsHeader(true);
        csvReader.setTextDelimiter(';');
        csvReader.setFieldSeparator(';');
        csvReader.setSkipEmptyRows(true);

        try (CsvParser csvParser = csvReader.parse(file, StandardCharsets.UTF_8)) {
            CsvRow row = csvParser.nextRow();
            for (String column : csvParser.getHeader()) {
                this.inputData.put(column, new ArrayList<>());
                this.log().info(String.format("Input file column detected %s", column));
            }
            do {
                for (String column : this.inputData.keySet()) {
                    this.inputData.get(column).add(row.getField(column));
                }
            } while ((row = csvParser.nextRow()) != null);
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SlaveNodeRegistrationMessage.class, this::handle)
                .match(SlaveNodeTerminatedMessage.class, this::handle)
                .match(PasswordsCrackedMessage.class, this::handle)
                .match(BestGenePartnersFoundMessage.class, this::handle)
		        .match(LinearCombinationFoundMessage.class, this::handle)
                .match(DispatcherMessages.ReleaseComputationNodeMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
                .build();
    }

    private void handle(SlaveNodeRegistrationMessage message) {
        this.currentNumberOfSlaves++;
        this.pipeline.addWorkers(message.getSlaveAddress(), message.getNumberOfWorkers());

        if (this.minNumberOfSlavesToStartWork == this.currentNumberOfSlaves) {
            this.pipeline.start();
        }
    }

    private void handle(SlaveNodeTerminatedMessage message) {
        this.log().warning(String.format("Slave actor %s terminated", message.getSlaveAddress()));
    }

    private void handle(DispatcherMessages.ReleaseComputationNodeMessage message) {
        this.pipeline.addWorker(message.getWorkerAddress());
    }

    private void handle(PasswordsCrackedMessage message) {
        this.pipeline.passwordCrackingFinished(message.getPlainPasswords());
    }

    private void handle(BestGenePartnersFoundMessage message) {
        this.pipeline.geneAnalysisFinished(message.getBestGenePartners());
    }

    private void handle(LinearCombinationFoundMessage message) {
		this.pipeline.linearCombincationFinished(message.prefixes);
    }

    private void handle(Terminated message) {

        final ActorRef sender = this.getSender();

        this.log().warning("{} has terminated.", sender);

        if (this.pipeline.hasFinished()) {
            this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
        }
    }
}