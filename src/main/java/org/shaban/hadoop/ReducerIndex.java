package org.shaban.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ReducerIndex extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();
    Set<String> documentsOfFirstWord = new HashSet<>();
    Set<String> documentsOfSecondWord = new HashSet<>();

    String firstWord = "";
    String secondWord = "";
    private boolean firstIteration = true;
    private boolean secondIteration = true;


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (firstIteration) {
            for (Text value : values) {
                firstWord = key.toString();
                documentsOfFirstWord.add(value.toString());
            }

            String wordWithDocs = key.toString() + " appears in: \t" + String.join(",", documentsOfFirstWord);
            context.write(new Text(wordWithDocs), result);

            firstIteration = false;
        } else if (secondIteration) {
            for (Text value : values) {
                secondWord = key.toString();
                documentsOfSecondWord.add(value.toString());
            }

            String wordWithDocs = key.toString() + " appears in: \t" + String.join(",", documentsOfSecondWord);
            context.write(new Text(wordWithDocs), result);

            secondIteration = false;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String word1 = context.getConfiguration().get(Constant.QUERY_WORD_1);
        String word2 = context.getConfiguration().get(Constant.QUERY_WORD_2);

        String andResult;
        String orResult;

        if (documentsOfSecondWord.isEmpty() || documentsOfFirstWord.isEmpty()) {
            andResult = word1 + " AND " + word2 + " result is: \t" + " There is no intersection between the two words ";
        } else {
            andResult = WordOperation.performAND(firstWord, secondWord, documentsOfFirstWord, documentsOfSecondWord);
        }
        context.write(new Text(andResult), result);
        if (documentsOfSecondWord.isEmpty() && documentsOfFirstWord.isEmpty()) {
            orResult = word1 + " OR " + word2 + " result is: \t" + "the two words not appears ";
        } else if (documentsOfSecondWord.isEmpty()) {
            orResult = word1 + " OR " + word2 + " result is: \t" + String.join(", ", documentsOfFirstWord);
        } else if (documentsOfFirstWord.isEmpty()) {
            orResult = word1 + " OR " + word2 + " result is: \t" + String.join(", ", documentsOfSecondWord);
        } else {
            orResult = WordOperation.performOR(firstWord, secondWord, documentsOfFirstWord, documentsOfSecondWord);
        }
        context.write(new Text(orResult), result);
    }
}
