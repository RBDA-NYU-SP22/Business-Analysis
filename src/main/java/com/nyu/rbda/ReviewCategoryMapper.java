package com.nyu.rbda;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;


public class ReviewCategoryMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
            throws IOException, InterruptedException {
        JSONObject jsonObject = new JSONObject(value.toString());
        int review_count = Integer.valueOf(jsonObject.get("review_count").toString());
        int review_category = (int)(review_count/50)+1;
        context.write(new IntWritable(review_category), new IntWritable(1));
    }
    
}
