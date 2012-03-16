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

public class MaxEntropyRowReducer extends Reducer<IntWritable, StatisticWritable, IntWritable, DoubleWritable> {

	private Map<Integer, FastIDSet> itemGroupMap;
	private Path pathToItemPrefs;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		itemGroupMap = new HashMap<Integer, FastIDSet>();
		String tempDir = context.getConfiguration().get(Config.TEMP_DIR);
		String itemPrefsDir = context.getConfiguration().get(Config.ITEM_PREFS_DIR);
		pathToItemPrefs = new Path(tempDir, itemPrefsDir);
		DataLoader.LoadGroupMap(pathToItemPrefs, context.getConfiguration(), itemGroupMap);
	}

	@Override
	protected void reduce(IntWritable groupID, Iterable<StatisticWritable> stats, Context context) throws IOException,
			InterruptedException {
		Statistic stat = new Statistic();
		for (StatisticWritable sw : stats) {
			stat.AddStat(sw.get());
		}
		double entropy_per_row = EntropyPerRow(stat, itemGroupMap);
		context.write(groupID, new DoubleWritable(entropy_per_row));
	}

	private double EntropyPerRow(Statistic sw, Map<Integer, FastIDSet> map) {
		double entropy = 0.0;
		long bj = 0;
		for (FastIDSet set : map.values()) {
			bj += set.size();
		}
		for (Map.Entry<Integer, FastIDSet> entry : map.entrySet()) {
			entropy += InfoUtil.H(sw.getCountMap(entry.getKey()), sw.getNumTotalValues(entry.getKey()));
		}
		return entropy * bj;
	}
}