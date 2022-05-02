package com.nyu.rbda;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BusinessStarRankReducer extends Reducer<Text, Text, NullWritable, Text>{
    HashMap<String, PriorityQueue<Entry<Double, String>>> stateMap;
    @Override
    protected void setup(Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        stateMap = new HashMap<>();
    }
    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
                String state = key.toString();
        for(Text val: values) {
            String[] s = val.toString().split(" ");
            double stars = Double.parseDouble(s[0]);
            String name = s[1];
            if(!stateMap.containsKey(key.toString())) {
                stateMap.put(state, new PriorityQueue<Entry<Double, String>>((e1, e2)->e1.getKey()-e2.getKey()<0?-1:1));
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
