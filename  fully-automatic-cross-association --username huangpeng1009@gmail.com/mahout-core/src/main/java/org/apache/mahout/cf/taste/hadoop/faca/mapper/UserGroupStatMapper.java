package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.faca.Config;
import org.apache.mahout.cf.taste.hadoop.faca.data.DataLoader;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAFromUserWithGNWritable;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;
import org.apache.mahout.cf.taste.hadoop.faca.data.StatisticWritable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.Preference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserGroupStatMapper extends Mapper<LongWritable, PAFromUserWithGNWritable, IntWritable, StatisticWritable> {

	private Logger log = LoggerFactory.getLogger(UserGroupStatMapper.class);
	private Map<Integer, FastIDSet> itemGroupMap;
	
	@Override
	protected void setup(Context context) throws IOException{
		itemGroupMap = new HashMap<Integer, FastIDSet>();
		Configuration conf = context.getConfiguration();
		String tempDir = conf.get(Config.TEMP_DIR);
		String itemStatDir = conf.get(Config.IG_STAT_DIR);
		Path path = new Path(tempDir, itemStatDir);
		DataLoader.LoadGroupMap(path, conf, itemGroupMap);
	}
	
	@Override
	protected void map(LongWritable userID, PAFromUserWithGNWritable prefs, Context context) throws IOException,
			InterruptedException {

		int groupID = prefs.getUserGroupID();
		Statistic stat = new Statistic();
		for (Preference pref : prefs.getPreferenceArray()) {
			stat.IncrNumValue(getItemGroupID(pref.getItemID()), pref.getValue(), 1);
		}
		stat.AddID(userID.get());
		log.info(stat.toString());
		context.write(new IntWritable(groupID), new StatisticWritable(stat));
	}
	
	private int getItemGroupID(long itemID){
		for(int groupID : itemGroupMap.keySet()){
			if(itemGroupMap.get(groupID).contains(itemID)){
				return groupID;
			}
		}
		return (int)(itemID - 100) % 3 + 1;
	}
}
