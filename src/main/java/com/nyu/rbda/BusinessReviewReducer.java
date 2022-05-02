package com.nyu.rbda;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONArray;
import org.json.JSONObject;

public class BusinessReviewReducer extends Reducer<Text, Text, NullWritable, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
            boolean whetherBusiness = false;
            JSONObject business = new JSONObject();
            for(Text val: values) {
                if(val.toString().startsWith("B")) {
                    whetherBusiness = true;
                    business = new JSONObject(val.toString().substring(2));
                    break;
                }
            }
            if(whetherBusiness) {
                for(Text val: values) {
                    if(val.toString().startsWith("R")) {
                        JSONObject review = new JSONObject(val.toString().substring(2));
                        if(business.has("state")&&business.has("stars")&&business.has("name")) {
                            review.put("state", business.get("state").toString());
                            review.put("business_stars", business.get("stars"));
                            review.put("name", business.get("name").toString());
                            context.write(NullWritable.get(), new Text(review.toString()));
                        }
                    }
                }
            }
        }
    }
    
    

