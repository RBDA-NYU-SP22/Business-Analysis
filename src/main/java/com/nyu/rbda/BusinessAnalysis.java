package com.nyu.rbda;

import java.util.Arrays;

import com.nyu.rbda.BusinessCleaning.BusinessFilterMapper;
import com.nyu.rbda.BusinessCleaning.BusinessFilterReducer;
import com.nyu.rbda.BusinessCleaning.ReviewFilterMapper;
import com.nyu.rbda.BusinessProfiling.AttributeMapper;
import com.nyu.rbda.BusinessProfiling.AttributeReducer;
import com.nyu.rbda.BusinessProfiling.BusinessCountMapper;
import com.nyu.rbda.BusinessProfiling.BusinessCountReducer;
import com.nyu.rbda.BusinessProfiling.CategoryStatisticMapper;
import com.nyu.rbda.BusinessProfiling.CategoryStatisticReducer;
import com.nyu.rbda.BusinessProfiling.StateBusinessCountMapper;
import com.nyu.rbda.BusinessProfiling.StateBusinessCountReducer;
import com.nyu.rbda.BusinessProfiling.UniqueCheckMapper;
import com.nyu.rbda.BusinessProfiling.UniqueCheckReducer;
import com.nyu.rbda.Integration.BusinessReviewFilterMapper;
import com.nyu.rbda.Integration.BusinessReviewReducer;
import com.nyu.rbda.Integration.BusinessReviewStarsReducer;
import com.nyu.rbda.Integration.BusinessStarRankMapper;
import com.nyu.rbda.Integration.BusinessStarRankReducer;
import com.nyu.rbda.Integration.FilterBusinessWithReviewReducer;
import com.nyu.rbda.Integration.ReviewUserMapper;
import com.nyu.rbda.Integration.ReviewUserReducer;
import com.nyu.rbda.Integration.UserReviewMapper;
import com.nyu.rbda.ReviewCleaning.ReviewCategoryMapper;
import com.nyu.rbda.ReviewCleaning.ReviewCategoryReducer;
import com.nyu.rbda.ReviewCleaning.ReviewTokenizerMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BusinessAnalysis {
    public static String[] analysis_name = new String[]{"stateCount", "businessCount", "uniqueCheck", "attributeStatistic", "categoryStatistic", "filterBusiness", "reviewCountCategory", "businessStarRank", "reviewTokenizer", "businessReviewFilter", "businessReviewStarsRank", "filterBusinessWithReview", "userReviewAdjusted"};

    public static int findAnalysisIndex(String target) {
        for(int i=0; i<analysis_name.length; i++) {
            if(analysis_name[i].equals(target)) {
                return i;
            }
        }
        return -1;
    }
    public static void main(String[] args) throws Exception {
        if(args.length!=3&&args.length!=4) {
            System.out.println("BusinessAnalysis usage: BusinessAnalysis <input path> <output path> <analysis name>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        conf.set("mapreduce.textoutputformat.separator", " ");
        Path outputPath = args.length==3?new Path(args[1]):new Path(args[2]);
        
        FileSystem.getLocal(conf).delete(outputPath, true);
        job.setJarByClass(BusinessAnalysis.class);
        job.setNumReduceTasks(1);
        if(args.length==3) {
            FileInputFormat.addInputPath(job, new Path(args[0])); 
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
        } else {
            FileOutputFormat.setOutputPath(job, outputPath);
        }
        int analysisIndex = args.length==3?findAnalysisIndex(args[2]):findAnalysisIndex(args[3]);
        if(analysisIndex==0) {    
            job.setJobName("State Count");
            job.setMapperClass(StateBusinessCountMapper.class);
            job.setReducerClass(StateBusinessCountReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            System.exit(job.waitForCompletion(true)?0:1);
        } else if(analysisIndex==1) {
            job.setJobName("Business Count");
            job.setMapperClass(BusinessCountMapper.class);
            job.setReducerClass(BusinessCountReducer.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(IntWritable.class);
            System.exit(job.waitForCompletion(true)?0:1);
        } else if(analysisIndex==2) {
            job.setJobName("Unique Business Id Check");
            job.setMapperClass(UniqueCheckMapper.class);
            job.setReducerClass(UniqueCheckReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            System.exit(job.waitForCompletion(true)?0:1);
        } else if(analysisIndex==3) {
            job.setJobName("Attribute Statistic");
            job.setMapperClass(AttributeMapper.class);
            job.setReducerClass(AttributeReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            System.exit(job.waitForCompletion(true)?0:1);
        }  else if(analysisIndex==4) {
            job.setJobName("Category Statistic");
            job.setMapperClass(CategoryStatisticMapper.class);
            job.setReducerClass(CategoryStatisticReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            System.exit(job.waitForCompletion(true)?0:1);
            
        }  else if(analysisIndex==5) {
            job.setJobName("Filter Business");
            job.setMapperClass(BusinessFilterMapper.class);
            job.setCombinerClass(BusinessFilterReducer.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setMapOutputKeyClass(NullWritable.class);
            job.setMapOutputValueClass(Text.class);
            System.exit(job.waitForCompletion(true)?0:1);
        }  else if(analysisIndex==6) {
            job.setJobName("Review Count Category");
            job.setMapperClass(ReviewCategoryMapper.class);
            job.setReducerClass(ReviewCategoryReducer.class);
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            System.exit(job.waitForCompletion(true)?0:1);
        }  else if(analysisIndex==7) {
            job.setJobName("Business Star Rank");
            job.setMapperClass(BusinessStarRankMapper.class);
            job.setReducerClass(BusinessStarRankReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            System.exit(job.waitForCompletion(true)?0:1);
        }  else if(analysisIndex==8) {
            job.setJobName("Review Tokenize");
            job.setMapperClass(ReviewTokenizerMapper.class);
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(MapWritable.class);
            System.exit(job.waitForCompletion(true)?0:1);
        }  else if(analysisIndex==9){
            job.setJobName("Business Review Filter");
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReviewFilterMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BusinessReviewFilterMapper.class);
            job.setReducerClass(BusinessReviewReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            System.exit(job.waitForCompletion(true)?0:1);
        }  else if(analysisIndex==10) {
            job.setJobName("Business Review Stars Rank");
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReviewFilterMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BusinessReviewFilterMapper.class);
            job.setReducerClass(BusinessReviewStarsReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            System.exit(job.waitForCompletion(true)?0:1);
        }   else if(analysisIndex==11) {
            job.setJobName("Filter Business With Review");
            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ReviewFilterMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, BusinessReviewFilterMapper.class);
            job.setReducerClass(FilterBusinessWithReviewReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            System.exit(job.waitForCompletion(true)?0:1);
        }  else if(analysisIndex==12) {
            job.setJobName("User Review Rate Adjusted");
            FileSystem fs = FileSystem.get(conf);
            RemoteIterator<LocatedFileStatus> fileStatusListIterator = fs.listFiles(new Path(args[0]), false);
            while(fileStatusListIterator.hasNext()) {
                LocatedFileStatus locatedFileStatus = fileStatusListIterator.next();
                String name = locatedFileStatus.getPath().getName();
                if(name.contains("part-m-")) {
                    MultipleInputs.addInputPath(job, locatedFileStatus.getPath(), TextInputFormat.class, UserReviewMapper.class);
                }
            }
            MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReviewUserMapper.class);
            job.setReducerClass(ReviewUserReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            System.exit(job.waitForCompletion(true)?0:1);
        }  else {
            System.out.println("Wrong analysis name, it should be in set: "+Arrays.asList(analysis_name).toString());
            System.exit(-1);
        }
    }
    
}
