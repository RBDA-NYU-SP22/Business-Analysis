package com.nyu.rbda.Integration;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class BusinessReviewFilterMapper extends Mapper<LongWritable, Text, Text, Text>{
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
        JSONObject jsonObject = new JSONObject(value.toString());
        String business_id = jsonObject.get("business_id").toString();
        context.write(new Text(business_id), new Text("B "+value.toString()));
        
    };
    
}
