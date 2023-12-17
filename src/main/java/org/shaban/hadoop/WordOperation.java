package org.shaban.hadoop;

import java.util.Set;

public class WordOperation {

    public static String performAND(String firstWord, String secondWord, Set<String> firstDocument, Set<String> secondDocument) {
        firstDocument.retainAll(secondDocument);

        return firstWord + " AND " + secondWord + " result is: \t" + String.join(", ", firstDocument);
    }

    public static String performOR(String firstWord, String secondWord, Set<String> firstDocument, Set<String> secondDocument) {
        firstDocument.addAll(secondDocument);

        return firstWord + " OR " + secondWord + " result is: \t" + String.join(", ", firstDocument);
    }
}

