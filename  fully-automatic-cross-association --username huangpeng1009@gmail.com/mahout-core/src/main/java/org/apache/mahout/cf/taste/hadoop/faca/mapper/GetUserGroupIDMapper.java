package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GetUserGroupIDMapper extends Mapper<IntWritable, DoubleWritable, IntWritable, Text>{

	@Override
	protected void map(IntWritable key, DoubleWritable value, Context context) throws IOException, InterruptedException {
		String out_str = String.valueOf(key.get()) + "_" + String.valueOf(value.get());
		context.write(new IntWritable(0), new Text(out_str));
	}
	
}
