package org.shaban.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerIndex extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder documentsOfWord = new StringBuilder();
        for (Text value : values) {
            documentsOfWord.append(value.toString()).append(",");
        }

        String finalResult = documentsOfWord.toString().replaceAll(",$", "");

        result.set(finalResult);
        context.write(key, result);
    }
}

