package com.example.task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WeeklyFlowReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        long totalPurchase = 0;
        long totalRedeem = 0;
        int count = 0;

        for (Text value : values) {
            String[] amounts = value.toString().split(",");
            totalPurchase += Long.parseLong(amounts[0]);
            totalRedeem += Long.parseLong(amounts[1]);
            count++;
        }

        long avgPurchase = totalPurchase / count;
        long avgRedeem = totalRedeem / count;

        context.write(key, new Text(avgPurchase + "," + avgRedeem));
    }
}