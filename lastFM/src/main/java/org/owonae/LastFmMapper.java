package org.owonae;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


import java.io.IOException;

public class LastFmMapper extends MapReduceBase implements Mapper<Object, Text, IntWritable, IntWritable>{
    IntWritable trackId = new IntWritable();
    IntWritable userId = new IntWritable();

    @Override
    public void map(Object o, Text value, OutputCollector<IntWritable, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String[] parts = value.toString().split("[|]");
        trackId.set(Integer.parseInt(parts[LastFmConstants.track_Id]));
        userId.set(Integer.parseInt(parts[LastFmConstants.user_Id]));

        outputCollector.collect(trackId, userId);
    }
}
