package org.owonae;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class TempMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
    public static final int MISSING = 9999;

    @Override
    public void map(LongWritable arg0, Text value, OutputCollector<Text, Text>
            output, Reporter reporter) throws IOException {
        String line = value.toString();

        if(!(line.length()==0)){
            String date = line.substring(6,14);

            float temp_Max = Float.parseFloat(line.substring(39, 45).trim());
            float temp_Min = Float.parseFloat(line.substring(47, 53).trim());

            if(temp_Max>35 && temp_Max!=MISSING){
                output.collect(new Text("Hot Day "+ date),
                        new Text(String.valueOf(temp_Max)));
            }
            if(temp_Min<10 && temp_Min!=MISSING){
                output.collect(new Text("Cold Day "+ date),
                        new Text(String.valueOf(temp_Min)));
            }
        }
    }
}
