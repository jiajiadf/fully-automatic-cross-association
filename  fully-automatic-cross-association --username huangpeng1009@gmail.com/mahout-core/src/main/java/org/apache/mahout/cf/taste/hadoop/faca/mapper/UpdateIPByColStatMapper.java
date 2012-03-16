package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.faca.Config;
import org.apache.mahout.cf.taste.hadoop.faca.data.DataLoader;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAForItemWithGNWritable;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;

/**
 * @author Curry
 * Update item prefs by column group statistic
 */
public class UpdateIPByColStatMapper extends Mapper<LongWritable, PAForItemWithGNWritable, LongWritable, PAForItemWithGNWritable>{
	
	private Map<Integer, Statistic> statMap;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		statMap = new HashMap<Integer, Statistic>();
		Path pathToColStatDir = new Path(context.getConfiguration().get(Config.TEMP_DIR),
										 context.getConfiguration().get(Config.IG_STAT_DIR));
		DataLoader.LoadStat(pathToColStatDir, context.getConfiguration(), statMap);
	}
	
	@Override
	protected void map(LongWritable key, PAForItemWithGNWritable value, Context context) throws IOException, InterruptedException {
		for(Map.Entry<Integer, Statistic> entry : statMap.entrySet()){
			if(entry.getValue().getIDs().contains(key.get())){
				value.setItemGroupID(entry.getKey());
			}
		}
		context.write(key, value);
	}
}
