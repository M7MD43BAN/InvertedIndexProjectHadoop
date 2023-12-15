package org.shaban.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerIndex extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder fileList = new StringBuilder();

        for (Text value : values) {
            fileList.append(value.toString()).append(", ");
        }

        // Remove the trailing comma
        String resultString = fileList.toString().replaceAll(",$", "");

        // Output the result in the desired format
        result.set(resultString);
        context.write(key, result);
    }
}

