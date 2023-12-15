package org.shaban.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombinerIndex extends Reducer<Text, Text, Text, Text> {
    private final Text wordInDocument = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int wordIndex = key.toString().indexOf("@");

        wordInDocument.set(key.toString().substring(wordIndex + 1));
        key.set(key.toString().substring(0, wordIndex));
        context.write(key, wordInDocument);
    }
}

