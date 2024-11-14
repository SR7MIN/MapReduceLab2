package com.example.task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DailyFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(",");
        if (fields.length == 17) {
            String reportDate = fields[1];
            String totalPurchaseAmt = fields[5].isEmpty() ? "0" : fields[5];
            String totalRedeemAmt = fields[10].isEmpty() ? "0" : fields[10];
            context.write(new Text(reportDate), new Text(totalPurchaseAmt + "," + totalRedeemAmt));
        }
    }
}
