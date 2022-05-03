package com.nyu.rbda;

import java.io.IOException;

import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

public class ReviewUserReducer extends Reducer<Text, Text, NullWritable, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        double rate = 0.0;
        for(Text val: values) {
            if(val.toString().startsWith("A")) {
                rate = Double.parseDouble(val.toString());
                break;
            }
        }
        for(Text val: values) {
            if(val.toString().startsWith("R")) {
                JSONObject review = new JSONObject(val.toString());
                double stars = review.getDouble("stars");
                review.remove("stars");
                review.put("stars", stars*rate);
                context.write(NullWritable.get(), new Text(review.toString()));
            }

        }
    }
    
}
