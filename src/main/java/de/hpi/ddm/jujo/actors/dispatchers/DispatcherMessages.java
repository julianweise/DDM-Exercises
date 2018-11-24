package de.hpi.ddm.jujo.actors.dispatchers;

import akka.actor.Address;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

public class DispatcherMessages {

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class AddComputationNodeMessage implements Serializable {
        private static final long serialVersionUID = 7594619467258919392L;
        private Address[] workerAddresses;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class ReleaseComputationNodeMessage implements Serializable {
        private static final long serialVersionUID = -5301177795851229122L;
        private Address[] workerAddresses;
    }
}
