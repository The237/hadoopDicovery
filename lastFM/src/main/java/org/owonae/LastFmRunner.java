package org.owonae;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.*;

import java.io.IOException;


public class LastFmRunner {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        JobConf conf = new JobConf(LastFmRunner.class);

        conf.setJobName("Last Fm 1.0.0");

        conf.setMapOutputKeyClass(IntWritable.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(LastFmMapper.class);
        conf.setReducerClass(LastFmReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
