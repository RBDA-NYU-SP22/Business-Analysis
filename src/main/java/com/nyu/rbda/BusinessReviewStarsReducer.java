package com.nyu.rbda;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.JSONObject;

public class BusinessReviewStarsReducer extends Reducer<Text, Text, NullWritable, Text>{
    HashMap<String, PriorityQueue<Entry<Double, String>>> stateMap;
    @Override
    protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        stateMap = new HashMap<>();
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        double stars = 0.0;
        int cnt = 0;
        String name = "";
        String state = "";
        for(Text val: values) {
            if(val.toString().startsWith("B")) {
                JSONObject business = new JSONObject(val.toString().substring(2));
                name = business.get("name").toString();
                state = business.get("state").toString();
            } else {
                JSONObject review = new JSONObject(val.toString().substring(2));
                stars += review.getDouble("stars");
                cnt += 1;
            }
        }
        if(cnt>0) {
            stars = stars/cnt;
            if(!stateMap.containsKey(state)) {
                stateMap.put(state,new PriorityQueue<Entry<Double, String>>((e1, e2)->e1.getKey()-e2.getKey()<0?-1:1) );
            }

            PriorityQueue<Entry<Double, String>> priorityQueue = stateMap.get(state);
            
            priorityQueue.add(Map.entry(stars, name));
            if(priorityQueue.size()>10) {
                priorityQueue.poll();
            }
        }
    }
    @Override
    protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        for(Entry<String, PriorityQueue<Entry<Double, String>>> entry: stateMap.entrySet()) {
            PriorityQueue<Entry<Double, String>> priorityQueue = entry.getValue();
            String state = entry.getKey();
            for(Entry<Double, String> entry2: priorityQueue) {
                double stars = entry2.getKey();
                String name = entry2.getValue();
                context.write(NullWritable.get(), new Text(state+" "+name+" "+stars));
            }
        }
    }
    
}
