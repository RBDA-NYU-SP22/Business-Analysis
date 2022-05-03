package com.nyu.rbda;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FilterBusinessWithReviewReducer extends Reducer<Text, Text, NullWritable, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        boolean whetherReview = false;
        for(Text val: values) {
            if(val.toString().startsWith("R")) {
                whetherReview = true;
                break;
            }
        }
        if(whetherReview) {
            for(Text val: values) {
                if(val.toString().startsWith("B")) {
                    context.write(NullWritable.get(), new Text(val.toString().substring(2)));
                    break;
                }
            }

        }
    }
    
}
