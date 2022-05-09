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
    HashMap<String, PriorityQueue<String[]>> stateMap;
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
        double original_stars = 0.0;
        double review_adjusted_stars = 0.0;

        for(Text val: values) {
            if(val.toString().startsWith("B")) {
                JSONObject business = new JSONObject(val.toString().substring(2));
                name = business.get("name").toString();
                state = business.get("state").toString();
                original_stars = business.getDouble("stars");
            } else {
                JSONObject review = new JSONObject(val.toString().substring(2));
                stars += review.getDouble("stars");
                cnt += 1;
            }
        }
        if(!state.equals("")&&!name.equals("")&&cnt>0) {
            review_adjusted_stars = stars/cnt;
            stars = (stars/cnt)*Math.log(1.0*cnt/3.5+1)/3;
            if(!stateMap.containsKey(state)) {
                stateMap.put(state, new PriorityQueue<String[]>((e1, e2)->Double.parseDouble(e1[0])-Double.parseDouble(e2[0])<0?-1:1));
            }

            PriorityQueue<String[]> priorityQueue = stateMap.get(state);
            
            priorityQueue.add(new String[]{stars+"", name, review_adjusted_stars+"", original_stars+""});
            if(priorityQueue.size()>10) {
                priorityQueue.poll();
            }
        }
    }
    @Override
    protected void cleanup(Reducer<Text, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        for(Entry<String, PriorityQueue<String[]>> entry: stateMap.entrySet()) {
            PriorityQueue<String[]> priorityQueue = entry.getValue();
            String state = entry.getKey();
            while(!priorityQueue.isEmpty()) {
                String[] entry2 = priorityQueue.poll();
                double stars = Double.parseDouble(entry2[0]);
                double original_stars = Double.parseDouble(entry2[3]);
                double review_adjusted_stars = Double.parseDouble(entry2[2]);
                String name = entry2[1];
                context.write(NullWritable.get(), new Text(state+"|"+name+"|"+stars+"|"+review_adjusted_stars+"|"+original_stars));
            }
        }
    }
    
}
