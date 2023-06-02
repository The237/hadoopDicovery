package org.owonae;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

public class Purchase_Runner {

    public static void main(String[] args) throws IOException {
        JobConf conf = new JobConf(Purchase_Runner.class);

        conf.setJobName("Purchase job v1.0.0");

        // set the k,v output classes
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(FloatWritable.class);

        // set mapper and reducer classes
        conf.setMapperClass(Purchase_Mapper.class);
        conf.setReducerClass(Purchase_Reducer.class);

        // set input format class
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
    }
}
