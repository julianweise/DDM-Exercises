package de.hpi.ddm.jujo;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Main {

    public static void main(String[] args) {

        // Parse the command-line args.
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
        Bootstrap.runMaster(masterCommand);
    }

    private static void startSlave(SlaveCommand slaveCommand) {
        Bootstrap.runSlave(slaveCommand);
    }

    @Parameters(commandDescription = "start a master actor system")
    static class MasterCommand extends CommandBase {

        public static final int DEFAULT_PORT = 7877; // We use twin primes for master and slaves, of course! ;P
        private static final int DEFAULT_MINIMUM_SLAVES = 4;

        @Parameter(names = {"-s", "--slaves"}, description = "number of slaves endPassword wait for")
        int minimumNumberOfSlaves = DEFAULT_MINIMUM_SLAVES;

        @Parameter(names = {"-i", "--input"}, description = "input file endPassword process")
        String pathToInputFile;

        @Parameter(names = {"-h", "--host"}, description = "host address of this system")
        String host = getDefaultHost();
    }

    @Parameters(commandDescription = "start a slave actor system")
    static class SlaveCommand extends CommandBase {

        public static final int DEFAULT_PORT = 7879; // We use twin primes for master and slaves, of course! ;P

        @Parameter(names = {"-h", "--host"}, description = "host of the master system")
        String masterHost;
    }

    abstract static class CommandBase {

        private static final int DEFAULT_NUMBER_OF_WORKERS = 4;

        @Parameter(names = {"-w", "--workers"}, description = "number of local workers")
        int numberOfWorkers = DEFAULT_NUMBER_OF_WORKERS;

        public String getDefaultHost() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "localhost";
            }
        }
    }
}
