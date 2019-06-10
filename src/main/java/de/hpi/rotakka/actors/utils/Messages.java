package de.hpi.rotakka.actors.utils;


import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


public class Messages {
    @Data
    @NoArgsConstructor
    public final static class RegisterMe implements Serializable {
        public static final long serialVersionUID = 1L;
    }

    @Data
    @NoArgsConstructor
    public final static class UnregisterMe implements Serializable {
        public static final long serialVersionUID = 1L;
    }

}
