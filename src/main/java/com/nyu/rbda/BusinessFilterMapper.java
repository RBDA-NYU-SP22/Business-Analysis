package com.nyu.rbda;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

public class BusinessFilterMapper extends Mapper<LongWritable, Text, NullWritable, Text>{
    @Override
    protected void map(LongWritable key, Text value,
            Mapper<LongWritable, Text, NullWritable, Text>.Context context)
            throws IOException, InterruptedException {
        JSONObject jsonObject = new JSONObject(value.toString());
        String stateName = jsonObject.get("state").toString();
        HashSet<String> filterStateSet = new HashSet<String>();
        filterStateSet.addAll(Arrays.asList(new String[]{"MI", "MT", "NC", "SD", "UT", "VI", "VT", "XMS", "HI", "MA", "WA", "CO", "TX", "IL", "DE"}));
        HashSet<String> filterCategorySet = new HashSet<String>();
        filterCategorySet.addAll(Arrays.asList(new String[]{"Food", "Restaurants", "Bars", "Sandwiches", "Pizza", "Coffee & Tea", "Fast Food", "Breakfast & Brunch", "Burgers", "Specialty Food", "Seafood", "Desserts", "Bakeries", "Salad", "Chicken Wings", "Cafes", "Ice Cream & Frozen Yogurt", "Caterers", "Beer", "Wine & Spirits", "Cocktail Bars", "Sushi Bars", "Barbeque", "Juice Bars & Smoothies", "Steakhouses", "Diners", "Food Trucks", "Wine Bars", "Vegetarian", "Donuts", "Soup", "Tacos", "Bagels", "Hot Dogs", "Ethnic Food", "Cheesesteaks", "Cupcakes", "Noodles", "Fish & Chips", "Kebab"}));
        String keyName = "categories";
        String splitter = ", ";
        String[] categories = jsonObject.get(keyName).toString().split(splitter);
        boolean whetherRestaurant = true;
        for(int i=0; i<categories.length; i++) {
            if(!filterCategorySet.contains(categories[i])) {
                whetherRestaurant = false;
                break;
            }
        }
        if(!filterStateSet.contains(stateName)&&whetherRestaurant) {
            context.write(NullWritable.get(), value);
        }
                
    }
    
}
