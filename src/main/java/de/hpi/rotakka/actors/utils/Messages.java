package de.hpi.rotakka.actors.utils;


import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;


public class Messages {
    @Data
    @AllArgsConstructor
    public final static class RegisterMe implements Serializable {
        public static final long serialVersionUID = 1L;
    }


}
