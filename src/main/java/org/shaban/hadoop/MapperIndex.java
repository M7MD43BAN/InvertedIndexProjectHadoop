package org.shaban.hadoop;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

public class MapperIndex extends Mapper<LongWritable, Text, Text, Text> {
    private final Text keyInfo = new Text();
    private final Text valueInfo = new Text();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        StringTokenizer tokenizer = new StringTokenizer(value.toString());

        while (tokenizer.hasMoreTokens()) {
            String fileName = split.getPath().getName().split("\\.")[0];

            keyInfo.set(tokenizer.nextToken() + "@" + fileName);
            valueInfo.set("1");

            context.write(keyInfo, valueInfo);
        }
    }
}