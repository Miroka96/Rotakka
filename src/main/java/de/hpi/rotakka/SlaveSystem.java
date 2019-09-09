package de.hpi.rotakka;

class SlaveSystem extends ClusterSystem {

	static final String ROLE = "slave";

	SlaveSystem(String host, int port, String masterhost, int masterport) {
		super(host, port, masterhost, masterport);
	}

	String getRoleName() {
		return ROLE;
	}

    void customStart() {
    }
}
