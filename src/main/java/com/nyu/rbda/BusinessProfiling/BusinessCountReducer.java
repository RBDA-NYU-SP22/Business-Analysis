package com.nyu.rbda.BusinessProfiling;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessCountReducer extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable>{
    @Override
    protected void reduce(NullWritable arg0, Iterable<IntWritable> values,
            Reducer<NullWritable, IntWritable, NullWritable, IntWritable>.Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable value: values) {
            sum += value.get();
        }
        context.write(NullWritable.get(), new IntWritable(sum));
    }
    
}
