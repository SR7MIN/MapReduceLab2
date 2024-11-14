package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.example.task1.DailyFlowMapper;
import com.example.task1.DailyFlowReducer;
import com.example.task2.WeeklyFlowMapper;
import com.example.task2.WeeklyFlowReducer;
import com.example.task3.UserActivityMapper;
import com.example.task3.UserActivityReducer;

public class MainDriver {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: MainDriver <task> <input path> <output path>");
            System.exit(-1);
        }

        String task = args[0];
        String inputPath = args[1];
        String outputPath = args[2];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, task);

        job.setJarByClass(MainDriver.class);

        switch (task) {
            case "dailyFlow":
                job.setMapperClass(DailyFlowMapper.class);
                job.setReducerClass(DailyFlowReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            case "weeklyFlow":
                job.setMapperClass(WeeklyFlowMapper.class);
                job.setReducerClass(WeeklyFlowReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                break;
            case "userActivity":
                job.setMapperClass(UserActivityMapper.class);
                job.setReducerClass(UserActivityReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);
                break;
            default:
                System.err.println("Invalid task: " + task);
                System.exit(-1);
        }

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
