package de.hpi.rotakka;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class MainApp {

    static final String ACTOR_SYSTEM_NAME = "rotakka";
	
	public static void main(String[] args) {

    	MasterCommand masterCommand = new MasterCommand();
        SlaveCommand slaveCommand = new SlaveCommand();
        JCommander jCommander = JCommander.newBuilder()
                .addCommand(MasterSystem.ROLE, masterCommand)
                .addCommand(SlaveSystem.ROLE, slaveCommand)
            .build();

        try {
            jCommander.parse(args);

            if (jCommander.getParsedCommand() == null) {
                throw new ParameterException("No command given.");
            }

            ClusterSystem system;
            switch (jCommander.getParsedCommand()) {
                case MasterSystem.ROLE:
                    system = new MasterSystem(masterCommand.host, masterCommand.port);
                    system.start();
                    break;
                case SlaveSystem.ROLE:
                    system = new SlaveSystem(slaveCommand.host, slaveCommand.port, slaveCommand.masterhost, slaveCommand.masterport);
                    system.start();
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

    abstract static class CommandBase {

        static final int DEFAULT_MASTER_PORT = 7877;
        static final int DEFAULT_SLAVE_PORT = 7879;
    	
    	@Parameter(names = {"-h", "--host"}, description = "this machine's host name or IP to bind against")
        String host = this.getDefaultHost();

        String getDefaultHost() {
            try {
                return InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                return "localhost";
            }
        }
    	
        @Parameter(names = {"-p", "--port"}, description = "port to bind against", required = false)
        int port = this.getDefaultPort();

        abstract int getDefaultPort();
    }

    @Parameters(commandDescription = "start a master actor system")
    static class MasterCommand extends CommandBase {

        @Override
        int getDefaultPort() {
            return DEFAULT_MASTER_PORT;
        }
    }

    @Parameters(commandDescription = "start a slave actor system")
    static class SlaveCommand extends CommandBase {

        @Override
        int getDefaultPort() {
            return DEFAULT_SLAVE_PORT;
        }
        
        @Parameter(names = {"-mp", "--masterport"}, description = "port of the master", required = false)
        int masterport = DEFAULT_MASTER_PORT;

        @Parameter(names = {"-mh", "--masterhost"}, description = "host name or IP of the master", required = true)
        String masterhost;
    }
}
