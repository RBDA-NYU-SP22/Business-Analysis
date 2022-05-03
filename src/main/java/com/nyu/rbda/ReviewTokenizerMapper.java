package com.nyu.rbda;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

import opennlp.tools.tokenize.SimpleTokenizer;

public class ReviewTokenizerMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
    HashMap<String, Integer> map;
    String businessID;
    String reviewID;
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, MapWritable>.Context context)
            throws IOException, InterruptedException {
                map = new HashMap<>();
    }
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MapWritable>.Context context)
            throws IOException, InterruptedException {
                SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
                JSONObject jsonObject = new JSONObject(value.toString());
                String[] words = tokenizer.tokenize(jsonObject.get("text").toString());
                for(int i=0; i<words.length; i++) {
                    map.put(words[i], map.getOrDefault(words[i], 0)+1);
                }
                businessID = jsonObject.get("business_id").toString();
                reviewID = jsonObject.get("review_id").toString();
            MapWritable mapWritable = new MapWritable();
        
            for(Entry<String, Integer> entry: map.entrySet()) {
                String word = entry.getKey();
                int num = entry.getValue();
                mapWritable.put(new Text(word), new IntWritable(num));
            } 
            context.write(new Text(reviewID+" "+businessID), mapWritable);
            }
    
}
