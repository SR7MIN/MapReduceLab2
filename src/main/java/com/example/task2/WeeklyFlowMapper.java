package com.example.task2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class WeeklyFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private static final SimpleDateFormat dayFormat = new SimpleDateFormat("EEEE");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        if (fields.length == 2) {
            String date = fields[0];
            String amounts = fields[1];
            try {
                Date reportDate = dateFormat.parse(date);
                String weekday = dayFormat.format(reportDate);
                context.write(new Text(weekday), new Text(amounts));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}