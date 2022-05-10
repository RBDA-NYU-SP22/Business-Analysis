package com.nyu.rbda.BusinessProfiling;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueCheckReducer extends Reducer<Text, IntWritable, NullWritable, Text>{
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Reducer<Text, IntWritable, NullWritable, Text>.Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), key);
    }
}
