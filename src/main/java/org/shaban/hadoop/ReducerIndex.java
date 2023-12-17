package org.shaban.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ReducerIndex extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();
    Set<String> documentsOfFirstWord = new HashSet<>();
    Set<String> documentsOfSecondWord = new HashSet<>();

    String firstWord = "";
    String secondWord = "";
    private boolean lastMapperTask = false;
    private boolean firstIteration = true;
    private boolean secondIteration = true;


    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if (firstIteration) {
            System.out.println("First iteration");

            for (Text value : values) {
                firstWord = key.toString();
                documentsOfFirstWord.add(value.toString());
            }

            System.out.println("documentsOfFirstWord: " + documentsOfFirstWord);
            System.out.println("wordsList: " + firstWord);

            String wordWithDocs = key.toString() + " appears in: \t" + String.join(",", documentsOfFirstWord);
            context.write(new Text(wordWithDocs), result);

            firstIteration = false;
        } else if (secondIteration) {
            System.out.println("Second iteration");

            for (Text value : values) {
                secondWord = key.toString();
                documentsOfSecondWord.add(value.toString());
            }

            System.out.println("documentsOfSecondWord: " + documentsOfSecondWord);
            System.out.println("wordsList: " + secondWord);

            String wordWithDocs = key.toString() + " appears in: \t" + String.join(",", documentsOfSecondWord);
            context.write(new Text(wordWithDocs), result);

            secondIteration = false;
            lastMapperTask = true;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (lastMapperTask) {
            System.out.println("This is the last mapper task");

            String andResult = WordOperation.performAND(firstWord, secondWord, documentsOfFirstWord, documentsOfSecondWord);
            context.write(new Text(andResult), result);

            String orResult = WordOperation.performOR(firstWord, secondWord, documentsOfFirstWord, documentsOfSecondWord);
            context.write(new Text(orResult), result);
        }
    }
}

