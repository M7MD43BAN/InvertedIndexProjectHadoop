package org.shaban.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ReducerIndex extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    //create two documents for and operation
    Set<String> documentsOfFirstWord = new HashSet<>();
    Set<String> documentsOfSecondWord = new HashSet<>();

    //create two documents for or operation
    Set<String> firstDoc = new HashSet<>();
    Set<String> secondDoc = new HashSet<>();
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
                firstDoc.add(value.toString());
            }

            String wordWithDocs = key.toString() + " appears in: \t";
            result.set(String.join(",", documentsOfFirstWord));
            context.write(new Text(wordWithDocs), result);

            firstIteration = false;
        } else if (secondIteration) {
            for (Text value : values) {
                secondWord = key.toString();
                documentsOfSecondWord.add(value.toString());
                secondDoc.add(value.toString());
            }

            String wordWithDocs = key.toString() + " appears in: \t";
            result.set(String.join(",", documentsOfSecondWord));
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

        //words appears
        if (!word1.equals(firstWord) && !word1.equals(secondWord)) {
            String word1Appears = word1 + " result is: \t";
            result.set("this word not appears in files \t");

            context.write(new Text(word1Appears), result);
        }
        if (!word2.equals(firstWord) && !word2.equals(secondWord)) {
            String word2Appears = word2 + " result is: \t";
            result.set("this word not appears in files \t");

            context.write(new Text(word2Appears), result);
        }

        //and operation
        if (documentsOfSecondWord.isEmpty() || documentsOfFirstWord.isEmpty()) {
            andResult = word1 + " AND " + word2 + " result is: \t";
            result.set("There is no intersection between the two words");
        } else {
            result.set(WordOperation.performAND(documentsOfFirstWord, documentsOfSecondWord));
            andResult = firstWord + " AND " + secondWord + " result is: \t";
            if (documentsOfFirstWord.isEmpty()) {
                andResult = word1 + " AND " + word2 + " result is: \t";
                result.set("There is no intersection between the two words");
            }
        }
        context.write(new Text(andResult), result);

        //OR operation
        if (firstDoc.isEmpty()) {
            orResult = word1 + " OR " + word2 + " result is: \t";
            result.set("the two words not appears");
        } else if (secondDoc.isEmpty()) {
            orResult = word1 + " OR " + word2 + " result is: \t";
            result.set(String.join(", ", firstDoc));
        } else {
            result.set(WordOperation.performOR(firstDoc, secondDoc));
            orResult = word1 + " OR " + word2 + " result is: \t";
        }
        context.write(new Text(orResult), result);
    }
}
