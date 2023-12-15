package org.shaban.hadoop;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InvertIndexApp {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        if (args.length != 2) {
            System.err.println("Usage: InvertIndexApp <input path> <output path>");
            System.exit(-1);
        }

        String inputPath = args[0];
        String outputPath = args[1];

        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(configuration);

        // To avoid output error use the same directory
        boolean exists = fileSystem.exists(new Path(outputPath));
        if (exists) {
            fileSystem.delete(new Path(outputPath), true);
        }

        Job job = Job.getInstance(configuration, "Invert Index");

        job.setJarByClass(InvertIndexApp.class);
        job.setMapperClass(MapperIndex.class);
        job.setCombinerClass(CombinerIndex.class);
        job.setReducerClass(ReducerIndex.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}