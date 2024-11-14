package com.example.task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UserActivityMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final LongWritable one = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        if (fields.length == 17) {
            String userId = fields[0];
            long directPurchaseAmt = fields[6].isEmpty() ? 0 : Long.parseLong(fields[6]);
            long totalRedeemAmt = fields[10].isEmpty() ? 0 : Long.parseLong(fields[10]);

            if (directPurchaseAmt > 0 || totalRedeemAmt > 0) {
                context.write(new Text(userId), one);
            }
        }
    }
}
