package com.nyu.rbda;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class CategoryStatisticMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        JSONObject jsonObject = new JSONObject(value.toString());
        String keyName = "categories";
        String splitter = ", ";
        if(!jsonObject.isNull(keyName)) {
            String[] categories = jsonObject.get(keyName).toString().split(splitter);
            for(String category: categories) {
                context.write(new Text(category), new IntWritable(1));
            }
        }
    }
    
    
}
