package org.shaban.hadoop;

import java.util.HashSet;
import java.util.Set;

public class WordOperation {

    public static String performAND(String firstWord, String secondWord, Set<String> firstDocument, Set<String> secondDocument) {
        Set<String> firstWordSet = new HashSet<>(firstDocument);
        Set<String> secondWordSet = new HashSet<>(secondDocument);

        firstWordSet.retainAll(secondWordSet);

        return firstWord + " AND " + secondWord + " result is: \t" + String.join(",", firstWordSet);
    }

    public static String performOR(String firstWord, String secondWord, Set<String> firstDocument, Set<String> secondDocument) {
        Set<String> result = new HashSet<>(firstDocument);
        result.addAll(secondDocument);

        return firstWord + " OR " + secondWord + " result is: \t" + String.join(",", result);
    }
}

