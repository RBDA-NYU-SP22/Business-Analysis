package com.nyu.rbda;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BusinessCountMapper extends Mapper<LongWritable, Text, NullWritable, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, IntWritable>.Context context)
            throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        String[] data = value.toString().split("	");
        int number = Integer.valueOf(data[1]);
        context.write(NullWritable.get(), new IntWritable(number));
    }
}
