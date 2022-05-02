package com.nyu.rbda;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReviewCategoryReducer extends Reducer<IntWritable, IntWritable, Text, IntWritable>{
    TreeMap<Integer, Integer> treeMap;
    @Override
    protected void setup(Reducer<IntWritable, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        treeMap = new TreeMap<Integer, Integer>();
    }
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> value,
            Reducer<IntWritable, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for(IntWritable val: value) {
            sum += val.get();
        }
        treeMap.put(key.get(), treeMap.getOrDefault(key.get(), 0)+sum);
    }
    @Override
    protected void cleanup(Reducer<IntWritable, IntWritable, Text, IntWritable>.Context context)
            throws IOException, InterruptedException {
        for(Entry<Integer, Integer> entry: treeMap.entrySet()) {
            int key = entry.getKey();
            String text = "Review count between "+((key-1)*50)+" and "+(key*50);
            context.write(new Text(text), new IntWritable(entry.getValue()));
        }
    }
}
