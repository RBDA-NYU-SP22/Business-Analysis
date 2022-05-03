package com.nyu.rbda;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class ReviewUserMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        JSONObject review = new JSONObject(value.toString());
        String review_id = review.getString("review_id");
        context.write(new Text(review_id), new Text("R "+value.toString()));
        
    }
    
}
