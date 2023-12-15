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

        String word1 = context.getConfiguration().get(Constant.QUERY_WORD_1);
        String word2 = context.getConfiguration().get(Constant.QUERY_WORD_2);

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();

            if (word.equals(word1)) {
                keyInfo.set(word1 + "@" + split.getPath().getName().split("\\.")[0]);
                context.write(keyInfo, valueInfo);
            }

            if (word.equals(word2)) {
                keyInfo.set(word2 + "@" + split.getPath().getName().split("\\.")[0]);
                context.write(keyInfo, valueInfo);
            }
        }
    }
}

