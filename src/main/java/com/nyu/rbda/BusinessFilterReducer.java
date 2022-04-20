package com.nyu.rbda;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessFilterReducer extends Reducer<NullWritable, Text, NullWritable, Text>{
    @Override
    protected void reduce(NullWritable key, Iterable<Text> value,
            Reducer<NullWritable, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        for(Text val: value) {
            context.write(NullWritable.get(), val);
        }
    }
    
    
}
