package de.hpi.ddm.jujo;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;


public class Main {

    public static void main(String[] args) {

        MasterCommand masterCommand = new MasterCommand();
        SlaveCommand slaveCommand = new SlaveCommand();
        JCommander jCommander = JCommander.newBuilder()
                .addCommand("master", masterCommand)
                .addCommand("slave", slaveCommand)
                .build();

        try {
            jCommander.parse(args);

            if (jCommander.getParsedCommand() == null) {
                throw new ParameterException("No command given.");
            }

            // Start a master or slave.
            switch (jCommander.getParsedCommand()) {
                case "master":
                    startMaster(masterCommand);
                    break;
                case "slave":
                    startSlave(slaveCommand);
                    break;
                default:
                    throw new AssertionError();

            }

        } catch (ParameterException e) {
            System.out.printf("Could not parse args: %s\n", e.getMessage());
            if (jCommander.getParsedCommand() == null) {
                jCommander.usage();
            } else {
                jCommander.usage(jCommander.getParsedCommand());
            }
            System.exit(1);
        }

    }

    private static void startMaster(MasterCommand masterCommand) throws ParameterException {
    }


    private static void startSlave(SlaveCommand slaveCommand) {
    }


    @Parameters(commandDescription = "start a master actor system")
    static class MasterCommand extends CommandBase {

        static final int PORT = 7877;
        static final int DEFAULT_WORKERS = 8;
        static final int DEFAULT_SLAVES = 4;

        @Override
        int getDefaultWorkers() {
            return DEFAULT_WORKERS;
        }

        @Parameter(names = {"-w", "--workers"}, description = "number of workers to start locally")
        int numLocalWorkers = 0;

        @Parameter(names = {"-s", "--slaves"}, description = "number of slaves to wait before beginning of calculation")
        int slaves = DEFAULT_SLAVES;

        @Parameter(names = {"-i", "--input"}, description = "input file to process")
        String file;
    }

    @Parameters(commandDescription = "start a slave actor system")
    static class SlaveCommand extends CommandBase {

        static final int DEFAULT_WORKERS = 20;

        @Override
        int getDefaultWorkers() {
            return DEFAULT_WORKERS;
        }

        @Parameter(names = {"-h", "--host"}, description = "host/IP to bind against")
        String host;
    }

    abstract static class CommandBase {

        @Parameter(names = {"-w", "--workers"}, description = "number of workers to spawn")
        int workers = getDefaultWorkers();

        abstract int getDefaultWorkers();
    }
}
