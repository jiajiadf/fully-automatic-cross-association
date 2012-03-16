package org.apache.mahout.cf.taste.hadoop.faca.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;
import org.apache.mahout.cf.taste.hadoop.faca.data.StatisticWritable;

public class UserReGroupReducer extends Reducer<IntWritable, StatisticWritable, IntWritable, StatisticWritable>{
	
	@Override
	protected void reduce(IntWritable userGroupID, Iterable<StatisticWritable> stats, Context context) throws IOException, InterruptedException {
		Statistic result = new Statistic();
		for(StatisticWritable sw : stats){
			Statistic stat = sw.get();
			result.AddStat(stat);
		}
		context.write(userGroupID, new StatisticWritable(result));
	}
}
