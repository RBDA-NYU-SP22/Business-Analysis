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
    HashMap<String, PriorityQueue<String>> stateMap;
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
                stateMap.put(state, new PriorityQueue<String>((e1, e2)->Double.parseDouble(e1.split("|")[0])-Double.parseDouble(e2.split("|")[0])<0?-1:1));
            }
            PriorityQueue<String> priorityQueue = stateMap.get(state);
            
            priorityQueue.add(stars+"|"+name);
            if(priorityQueue.size()>10) {
                priorityQueue.poll();
            }
        }

    }
    @Override
    protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        for(Entry<String, PriorityQueue<String>> entry: stateMap.entrySet()) {
            PriorityQueue<String> priorityQueue = entry.getValue();
            String state = entry.getKey();
            for(String entry2: priorityQueue) {
                String[] arr = entry2.split("|");
                double stars = Double.parseDouble(arr[0]);
                String name = arr[1];
                context.write(NullWritable.get(), new Text(state+" "+name+" "+stars));
            }
        }
    }
    
}
