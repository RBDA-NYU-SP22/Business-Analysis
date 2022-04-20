package com.nyu.rbda;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StateBusinessCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
    private TreeMap<Integer, List<String>> treeMap;
    @Override
    protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        treeMap = new TreeMap<>();
    }
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,
            Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        int sum = 0;
        for(IntWritable value: values) {
            sum += value.get();
        }
        if(!treeMap.containsKey(sum)) {
            treeMap.put(sum, new ArrayList<String>());
        }
        treeMap.get(sum).add(key.toString());
    }
    @Override
    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        for(Entry<Integer, List<String>> entry: treeMap.entrySet()) {
            int sum = entry.getKey();
            List<String> categories = entry.getValue();
            for(String state: categories) {
                context.write(new Text(state), new IntWritable(sum));
            }
        }
    }
    
    
}
