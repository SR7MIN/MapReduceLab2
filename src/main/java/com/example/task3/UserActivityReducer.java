package com.example.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class UserActivityReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long activeDays = 0;

        for (LongWritable value : values) {
            activeDays += value.get();
        }

        context.write(key, new LongWritable(activeDays));
    }
}
