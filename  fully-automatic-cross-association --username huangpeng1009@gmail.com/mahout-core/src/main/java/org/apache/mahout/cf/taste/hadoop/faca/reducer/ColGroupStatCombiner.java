package org.apache.mahout.cf.taste.hadoop.faca.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;
import org.apache.mahout.cf.taste.hadoop.faca.data.StatisticWritable;

public class ColGroupStatCombiner extends Reducer<IntWritable, StatisticWritable, IntWritable, StatisticWritable> {
	@Override
	protected void reduce(IntWritable groupID, Iterable<StatisticWritable> stats, Context context) throws IOException,
			InterruptedException {
		Statistic stat = new Statistic();
		for (StatisticWritable sw : stats) {
			stat.AddStat(sw.get());
		}
		context.write(groupID, new StatisticWritable(stat));
	}
}