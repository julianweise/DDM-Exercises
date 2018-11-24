package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.AbstractLoggingActor;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.Reaper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GeneWorker extends AbstractLoggingActor {

    public static Props props() {
        return Props.create(GeneWorker.class);
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class AddGeneSequencesMessage implements Serializable {
        private static final long serialVersionUID = 2670659815867593196L;
        private String[] geneSequences;
    }

    @Data @Builder @NoArgsConstructor @AllArgsConstructor
    public static class FindBestGenePartnerMessage implements Serializable {
        private static final long serialVersionUID = -2662447082397142212L;
        private int originalPerson;
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

        // Log the stop event
        this.log().info("Stopped {}.", this.getSelf());
    }

    private List<String> geneSequences = new ArrayList<>();

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(AddGeneSequencesMessage.class, this::handle)
                .match(FindBestGenePartnerMessage.class, this::handle)
                .matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
                .build();
    }

    private void handle(AddGeneSequencesMessage message) {
        this.geneSequences.addAll(Arrays.asList(message.geneSequences));
    }

    private void handle(FindBestGenePartnerMessage message) {
        int bestPartner = this.longestOverlapPartner(message.originalPerson);
        this.sender().tell(BestGenePartnerFoundMessage.builder()
                .originalPerson(message.originalPerson)
                .bestPartner(bestPartner)
                .build(),
            this.self()
        );
    }

    private int longestOverlapPartner(int thisIndex) {
        int bestOtherIndex = -1;
        String bestOverlap = "";
        for (int otherIndex = 0; otherIndex < this.geneSequences.size(); otherIndex++) {
            if (otherIndex == thisIndex)
                continue;

            String longestOverlap = this.longestOverlap(
                    this.geneSequences.get(thisIndex),
                    this.geneSequences.get(otherIndex)
            );

            if (bestOverlap.length() < longestOverlap.length()) {
                bestOverlap = longestOverlap;
                bestOtherIndex = otherIndex;
            }
        }
        return bestOtherIndex;
    }

    private String longestOverlap(String str1, String str2) {
        if (str1.isEmpty() || str2.isEmpty())
            return "";

        if (str1.length() > str2.length()) {
            String temp = str1;
            str1 = str2;
            str2 = temp;
        }

        int[] currentRow = new int[str1.length()];
        int[] lastRow = str2.length() > 1 ? new int[str1.length()] : null;
        int longestSubstringLength = 0;
        int longestSubstringStart = 0;

        for (int str2Index = 0; str2Index < str2.length(); str2Index++) {
            char str2Char = str2.charAt(str2Index);
            for (int str1Index = 0; str1Index < str1.length(); str1Index++) {
                int newLength;
                if (str1.charAt(str1Index) == str2Char) {
                    newLength = str1Index == 0 || str2Index == 0 ? 1 : lastRow[str1Index - 1] + 1;

                    if (newLength > longestSubstringLength) {
                        longestSubstringLength = newLength;
                        longestSubstringStart = str1Index - (newLength - 1);
                    }
                } else {
                    newLength = 0;
                }
                currentRow[str1Index] = newLength;
            }
            int[] temp = currentRow;
            currentRow = lastRow;
            lastRow = temp;
        }
        return str1.substring(longestSubstringStart, longestSubstringStart + longestSubstringLength);
    }
}
