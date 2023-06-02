package org.owonae;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class TempRunner {
    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(TempRunner.class);

        conf.setJobName("Temp 1.0.0");

        // config output K,V classes
        conf.setMapOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        // config Mapper and Reducer
        conf.setMapperClass(TempMapper.class);
        conf.setReducerClass(TempReducer.class);

        // config input and output formats
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        // Config FileInput and FileOutput formats
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}
