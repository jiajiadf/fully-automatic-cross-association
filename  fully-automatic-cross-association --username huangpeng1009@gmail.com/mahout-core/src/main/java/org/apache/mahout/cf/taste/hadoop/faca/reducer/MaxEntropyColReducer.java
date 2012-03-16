package org.apache.mahout.cf.taste.hadoop.faca.reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.faca.Config;
import org.apache.mahout.cf.taste.hadoop.faca.InfoUtil;
import org.apache.mahout.cf.taste.hadoop.faca.data.DataLoader;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;
import org.apache.mahout.cf.taste.hadoop.faca.data.StatisticWritable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;

public class MaxEntropyColReducer extends Reducer<IntWritable, StatisticWritable, IntWritable, DoubleWritable> {

	private Map<Integer, FastIDSet> userGroupMap;
	private Path pathToUserPrefs;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		userGroupMap = new HashMap<Integer, FastIDSet>();
		String tempDir = context.getConfiguration().get(Config.TEMP_DIR);
		String userPrefsDir = context.getConfiguration().get(Config.USER_PREFS_DIR);
		pathToUserPrefs = new Path(tempDir, userPrefsDir);
		DataLoader.LoadGroupMap(pathToUserPrefs, context.getConfiguration(), userGroupMap);
	}

	@Override
	protected void reduce(IntWritable groupID, Iterable<StatisticWritable> stats, Context context) throws IOException,
			InterruptedException {
		Statistic stat = new Statistic();
		for (StatisticWritable sw : stats) {
			stat.AddStat(sw.get());
		}
		double entropy_per_row = EntropyPerRow(stat, userGroupMap);
		context.write(groupID, new DoubleWritable(entropy_per_row));
	}

	private double EntropyPerRow(Statistic sw, Map<Integer, FastIDSet> map) {
		double entropy = 0.0;
		long ai = 0;
		for (FastIDSet set : map.values()) {
			ai += set.size();
		}
		for (Map.Entry<Integer, FastIDSet> entry : map.entrySet()) {
			entropy += InfoUtil.H(sw.getCountMap(entry.getKey()), sw.getNumTotalValues(entry.getKey()));
		}
		return entropy * ai;
	}
}
