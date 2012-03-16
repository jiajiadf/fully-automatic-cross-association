package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.faca.Config;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAFromUserWithGNWritable;

public class SplitRowStep1Mapper extends Mapper<LongWritable, PAFromUserWithGNWritable, IntWritable, NullWritable>{
	
	private int whichToSplit = 0;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		whichToSplit = context.getConfiguration().getInt(Config.GROUP_ID, 1);
	}
	
	@Override
	protected void map(LongWritable key, PAFromUserWithGNWritable value, Context context) throws IOException, InterruptedException {
		if(value.getUserGroupID() == whichToSplit){
			context.getCounter(Config.INFO, Config.SPLIT_POS).increment(1);
			int pos = (int)context.getCounter(Config.INFO, Config.SPLIT_POS).getValue();
			context.write(new IntWritable(pos), NullWritable.get());
		}
	}
}
