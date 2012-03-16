package org.apache.mahout.cf.taste.hadoop.faca.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.faca.Config;
import org.apache.mahout.cf.taste.hadoop.faca.data.FastIDSetSplit;
import org.apache.mahout.cf.taste.hadoop.faca.data.SplitWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitRowReducer extends Reducer<IntWritable, SplitWritable, IntWritable, SplitWritable>{
	
	private Logger log = LoggerFactory.getLogger(SplitRowReducer.class);
	
	@Override
	protected void reduce(IntWritable groupID, Iterable<SplitWritable> splits, Context context) throws IOException, InterruptedException {
		double result_gain = Double.MIN_VALUE;
		FastIDSetSplit split = null;
		for(SplitWritable sw : splits){
			double gain = sw.get().getGain();
			if(gain > result_gain){
				result_gain = gain;
				split = sw.get();
			}
		}
		if(split.getGain() != 0){
			context.getCounter(Config.INFO, Config.IS_SPLIT).increment(1);
		}
		context.write(groupID, new SplitWritable(split));
		log.info("groupID: " + groupID.get() + " " + split.toString());
	}
}
