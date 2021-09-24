package org.zju.mapreduce.maxtemperature;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * MaxTemperature Reducer
 *
 * @author zjx
 * @since 2021/9/24 下午3:31
 */
public class MaxTemperatureReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int maxValue = Integer.MIN_VALUE;
        for (IntWritable v : values) {
            maxValue = Math.max(maxValue, v.get());
        }
        context.write(key, new IntWritable(maxValue));
    }
}

