package org.owonae;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class LastFmReducer extends MapReduceBase implements Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
    @Override
    public void reduce(IntWritable trackId, Iterator<IntWritable> userIds,
                       OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
        Set<Integer> userIdSet = new HashSet<Integer>();
        for (Iterator<IntWritable> it = userIds; it.hasNext(); ) {
            IntWritable userId = it.next();
            userIdSet.add(userId.get());
        }
        IntWritable size = new IntWritable(userIdSet.size());
        output.collect(trackId, size);
    }
}
