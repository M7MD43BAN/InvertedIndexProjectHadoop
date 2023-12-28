package org.shaban.hadoop;

import java.util.Set;

public class WordOperation {

    public static String performAND(Set<String> firstDocument, Set<String> secondDocument) {
        firstDocument.retainAll(secondDocument);

        return String.join(", ", firstDocument);
    }

    public static String performOR(Set<String> firstDocument, Set<String> secondDocument) {
        firstDocument.addAll(secondDocument);

        return String.join(", ", firstDocument);
    }
}

