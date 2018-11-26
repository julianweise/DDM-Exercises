package de.hpi.ddm.jujo.actors.workers;

import akka.actor.AbstractActor;
import akka.actor.Props;
import de.hpi.ddm.jujo.actors.AbstractReapedActor;
import de.hpi.ddm.jujo.actors.dispatchers.GeneDispatcher;
import de.hpi.ddm.jujo.utils.SuffixArray;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GeneWorker extends AbstractReapedActor {

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

    private List<String> geneSequences = new ArrayList<>();

    @Override
    public AbstractActor.Receive createReceive() {
        return receiveBuilder()
                .match(AddGeneSequencesMessage.class, this::handle)
                .match(FindBestGenePartnerMessage.class, this::handle)
                .matchAny(this::handleAny)
                .build();
    }

    private void handle(AddGeneSequencesMessage message) {
        this.log().debug(String.format("Received %d gene sequences for gene analysis", message.geneSequences.length));
        this.geneSequences.addAll(Arrays.asList(message.geneSequences));
    }

    private void handle(FindBestGenePartnerMessage message) {
        this.log().debug(String.format("Received message to find best gene partner for person %d", message.originalPerson));
        int bestPartner = this.longestOverlapPartner(message.originalPerson);
        this.log().debug(String.format("Best gene partner for original person %d is %d", message.originalPerson, bestPartner));
        this.sender().tell(GeneDispatcher.BestGenePartnerFoundMessage.builder()
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

            String longestOverlap = longestCommonSubstring(
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

    /*
    Taken from: https://algs4.cs.princeton.edu/63suffix/LongestCommonSubstring.java.html
    */

    /**
     * Returns the longest common string of the two specified strings.
     *
     * @param  s one string
     * @param  t the other string
     * @return the longest common string that appears as a substring
     *         in both {@code s} and {@code t}; the empty string
     *         if no such string
     */
    public static String longestCommonSubstring(String s, String t) {
        SuffixArray suffix1 = new SuffixArray(s);
        SuffixArray suffix2 = new SuffixArray(t);

        // find longest common substring by "merging" sorted suffixes
        String lcs = "";
        int i = 0, j = 0;
        while (i < s.length() && j < t.length()) {
            int p = suffix1.index(i);
            int q = suffix2.index(j);
            String x = lcp(s, p, t, q);
            if (x.length() > lcs.length()) lcs = x;
            if (compare(s, p, t, q) < 0) i++;
            else                         j++;
        }
        return lcs;
    }

    // compare suffix s[p..] and suffix t[q..]
    private static int compare(String s, int p, String t, int q) {
        int n = Math.min(s.length() - p, t.length() - q);
        for (int i = 0; i < n; i++) {
            if (s.charAt(p + i) != t.charAt(q + i))
                return s.charAt(p+i) - t.charAt(q+i);
        }
        if      (s.length() - p < t.length() - q) return -1;
        else if (s.length() - p > t.length() - q) return +1;
        else                                      return  0;
    }

    // return the longest common prefix of suffix s[p..] and suffix t[q..]
    private static String lcp(String s, int p, String t, int q) {
        int n = Math.min(s.length() - p, t.length() - q);
        for (int i = 0; i < n; i++) {
            if (s.charAt(p + i) != t.charAt(q + i))
                return s.substring(p, p + i);
        }
        return s.substring(p, p + n);
    }
}
