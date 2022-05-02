package com.nyu.rbda;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class ReviewFilterMapper extends Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        JSONObject jsonObject = new JSONObject(value.toString());
        String business_id = jsonObject.get("business_id").toString();
        context.write(new Text(business_id), new Text("R "+value.toString()));
    }
    
}
