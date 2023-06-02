package org.owonae;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class Purchase_Mapper extends MapReduceBase implements Mapper<Object, Text, Text, FloatWritable> {

    Text payment_method = new Text();
    FloatWritable cost = new FloatWritable();
    @Override
    public void map(Object o, Text value, OutputCollector<Text, FloatWritable> output, Reporter reporter) throws IOException {
        String[] parts = value.toString().split("[\\t]");
        payment_method.set(parts[PurchaseConstants.payment_method]);
        cost.set(Float.parseFloat(parts[PurchaseConstants.cost]));

        output.collect(payment_method, cost);
    }
}