package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.faca.Config;
import org.apache.mahout.cf.taste.hadoop.faca.data.DataLoader;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAFromUserWithGNWritable;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;

/**
 * @author Curry
 * Update user prefs by row group statistic
 */
public class UpdateUPByRowStatMapper extends Mapper<LongWritable, PAFromUserWithGNWritable, LongWritable, PAFromUserWithGNWritable>{
	
	private Map<Integer, Statistic> statMap;
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Path pathToRowStatDir = new Path(context.getConfiguration().get(Config.TEMP_DIR),
										 context.getConfiguration().get(Config.UG_STAT_DIR));
		statMap = new HashMap<Integer, Statistic>();
		DataLoader.LoadStat(pathToRowStatDir, context.getConfiguration(), statMap);
	}
	
	@Override
	protected void map(LongWritable key, PAFromUserWithGNWritable value, Context context) throws IOException, InterruptedException {
		for(Map.Entry<Integer, Statistic> entry : statMap.entrySet()){
			if(entry.getValue().getIDs().contains(key.get())){
				value.setUserGroupID(entry.getKey());
			}
		}
		context.write(key, value);
	}
}