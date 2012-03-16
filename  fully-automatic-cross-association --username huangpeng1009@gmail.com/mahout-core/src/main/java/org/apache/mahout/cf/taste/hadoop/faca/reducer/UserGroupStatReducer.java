package org.apache.mahout.cf.taste.hadoop.faca.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;
import org.apache.mahout.cf.taste.hadoop.faca.data.StatisticWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserGroupStatReducer extends Reducer<IntWritable, StatisticWritable, IntWritable, StatisticWritable>{
	private Logger log = LoggerFactory.getLogger(UserGroupStatReducer.class);
	@Override
	protected void reduce(IntWritable groupID, Iterable<StatisticWritable> stats, Context context) throws IOException, InterruptedException {
		Statistic stat = new Statistic();
		for(StatisticWritable sw : stats){
			stat.AddStat(sw.get());
		}
		log.info(stat.toString());
		context.write(groupID, new StatisticWritable(stat));
	}
}