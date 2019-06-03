package de.hpi.rotakka;


class MasterSystem extends ClusterSystem {

    static final String ROLE = "master";

    MasterSystem(String host, int port) {
        super(host, port);
    }

    String getRoleName() {
        return ROLE;
    }


    void customStart() {


    }
}
