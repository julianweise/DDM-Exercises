package de.hpi.ddm.jujo.actors;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;

import akka.actor.*;
import de.hpi.ddm.jujo.actors.dispatchers.DispatcherMessages;
import de.hpi.ddm.jujo.actors.dispatchers.PasswordDispatcher;
import de.siegmar.fastcsv.reader.CsvParser;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.CsvRow;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class Master extends AbstractLoggingActor {

    public static final String DEFAULT_NAME = "master";
    private static final String INPUT_DATA_PASSWORD_COLUMN = "Password";

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


    private HashMap<Address, Integer> availableWorkersPerNode = new HashMap<>();
    private int minNumberOfSlaves;
    private Map<String, List<String>> inputData = new HashMap<>();

    public Master(int numLocalWorkers, int minNumberOfSlaves, String pathToInputFile) {
        try {
            this.parseInputFile(pathToInputFile);
        } catch (IOException e) {
            this.log().error(e, "Error while processing input file.");
        }
        this.minNumberOfSlaves = minNumberOfSlaves + 1; // local actor system counts as one slave
        this.self().tell(SlaveNodeRegistrationMessage.builder()
                .slaveAddress(this.self().path().address())
                .numberOfWorkers(numLocalWorkers)
                .build(),
                this.self()
        );
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
    public void preStart() throws Exception {
        super.preStart();

        // Register at this actor system's reaper
        Reaper.watchWithDefaultReaper(this);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();

        // If the master has stopped, it can also stop the listener
        // TODO: Kill PasswordMaster

        // Log the stop event
        this.log().info("Stopped {}.", this.getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(SlaveNodeRegistrationMessage.class, this::handle)
                .match(SlaveNodeTerminatedMessage.class, this::handle)
                .match(PasswordsCrackedMessage.class, this::handle)
                .match(BestGenePartnersFoundMessage.class, this::handle)
                .match(DispatcherMessages.ReleaseComputationNodeMessage.class, this::handle)
                .match(Terminated.class, this::handle)
                .matchAny(object -> this.log().info(this.getClass().getName() + " received unknown message: " + object.toString()))
                .build();
    }

    private void handle(SlaveNodeRegistrationMessage message) {
        this.log().info(String.format("New slave registered from %s", message.slaveAddress.toString()));
        this.availableWorkersPerNode.putIfAbsent(message.slaveAddress, message.numberOfWorkers);
        this.log().info(String.format("At least %d slaves required. Currently present number of slaves is %d", this.minNumberOfSlaves, this.availableWorkersPerNode.size()));
        if (this.minNumberOfSlaves == this.availableWorkersPerNode.size()) {
            ActorRef passwordDispatcher = this.context().system().actorOf(PasswordDispatcher.props(
                this.self(),
                this.inputData.get(INPUT_DATA_PASSWORD_COLUMN))
            );

            for(Map.Entry<Address, Integer> entry : this.availableWorkersPerNode.entrySet()) {
                passwordDispatcher.tell(
                        DispatcherMessages.AddComputationNodeMessage.builder()
                        .nodeAddress(entry.getKey())
                        .numberOfWorkers(entry.getValue())
                        .build(),
                    this.self()
                );
            }
        }
    }

    private void handle(SlaveNodeTerminatedMessage message) {
        this.availableWorkersPerNode.remove(message.slaveAddress);
    }

    private void handle(DispatcherMessages.ReleaseComputationNodeMessage message) {
        Address node = message.getNodeAddress();
        this.availableWorkersPerNode.putIfAbsent(node, 0);
        this.availableWorkersPerNode.computeIfPresent(node, (a, currentValue) -> currentValue + message.getNumberOfWorkers());
        // TODO Add method to redispatch available resources
    }

    private void handle(PasswordsCrackedMessage message) {
        this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.log().info(Arrays.toString(message.getPlainPasswords()));
    }

    private void handle(BestGenePartnersFoundMessage message) {
        this.sender().tell(PoisonPill.getInstance(), ActorRef.noSender());
        this.log().info(Arrays.toString(message.getBestGenePartners()));
    }

    private void handle(Terminated message) {

        // Find the sender of this message
        final ActorRef sender = this.getSender();

        // TODO

        this.log().warning("{} has terminated.", sender);

        // Check if work is complete and stop the actor hierarchy if true
        if (this.hasFinished()) {
            this.stopSelfAndListener();
        }
    }

    private boolean hasFinished() {
        // TODO
        return false;
    }

    private void stopSelfAndListener() {

        // Tell the listener endPassword stop
        // TODO

        // Stop self and all child actors by sending a poison pill
        this.getSelf().tell(PoisonPill.getInstance(), this.getSelf());
    }
}