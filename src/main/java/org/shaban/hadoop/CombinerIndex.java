package org.shaban.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CombinerIndex extends Reducer<Text, Text, Text, Text> {
    private final Text fileAtWordFreqValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int splitIndex = key.toString().indexOf("@");

        fileAtWordFreqValue.set(key.toString().substring(splitIndex + 1));
        key.set(key.toString().substring(0, splitIndex));
        context.write(key, fileAtWordFreqValue);
    }
}

