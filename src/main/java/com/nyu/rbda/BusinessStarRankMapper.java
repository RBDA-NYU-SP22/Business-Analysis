package com.nyu.rbda;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class BusinessStarRankMapper extends Mapper<LongWritable, Text, Text, Text>{
    HashMap<String, PriorityQueue<Entry<Double, String>>> stateMap;
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        stateMap = new HashMap<>();
    }
    protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
        JSONObject jsonObject = new JSONObject(value.toString());
        if(jsonObject.has("stars")) {
            double stars = Double.parseDouble(jsonObject.get("stars").toString());
            String state = jsonObject.get("state").toString();
            String name = jsonObject.get("name").toString();
            if(!stateMap.containsKey(state)) {
                stateMap.put(state, new PriorityQueue<Entry<Double, String>>((e1, e2)->e1.getKey()-e2.getKey()<0?-1:1));
            }
            PriorityQueue<Entry<Double, String>> priorityQueue = stateMap.get(state);
            
            priorityQueue.add(Map.entry(stars, name));
            if(priorityQueue.size()>10) {
                priorityQueue.poll();
            }
        }
    };
    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        for(Entry<String, PriorityQueue<Entry<Double, String>>> entry: stateMap.entrySet()) {
            PriorityQueue<Entry<Double, String>> priorityQueue = entry.getValue();
            String state = entry.getKey();
            for(Entry<Double, String> entry2: priorityQueue) {
                double stars = entry2.getKey();
                String name = entry2.getValue();
                context.write(new Text(state), new Text(stars+" "+name));
            }
        }
    }
}