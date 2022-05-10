package com.nyu.rbda.BusinessProfiling;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class AttributeMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        JSONObject jsonObject = new JSONObject(value.toString());
        if(!jsonObject.isNull("attributes")) {
            JSONObject attributeJSONObject = jsonObject.getJSONObject("attributes");
            for(String name: JSONObject.getNames(attributeJSONObject)) {
                context.write(new Text(name), new IntWritable(1));
            }
        }
    }
}
