package com.nyu.rbda;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;
public class StateBusinessCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        JSONObject jsonObject = new JSONObject(value.toString());
        String stateName = jsonObject.get("state").toString();
        context.write(new Text(stateName), new IntWritable(1));
    }
}
