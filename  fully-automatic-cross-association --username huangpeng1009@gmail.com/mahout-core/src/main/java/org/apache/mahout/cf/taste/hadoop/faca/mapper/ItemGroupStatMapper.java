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
import org.apache.mahout.cf.taste.hadoop.faca.data.PAForItemWithGNWritable;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;
import org.apache.mahout.cf.taste.hadoop.faca.data.StatisticWritable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.Preference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemGroupStatMapper extends Mapper<LongWritable, PAForItemWithGNWritable, IntWritable, StatisticWritable> {

	private Logger log = LoggerFactory.getLogger(ItemGroupStatMapper.class);
	private Map<Integer, FastIDSet> userGroupMap;
	
	@Override
	protected void setup(Context context) throws IOException {
		userGroupMap = new HashMap<Integer, FastIDSet>();
		Configuration conf = context.getConfiguration();
		String tempDir = conf.get(Config.TEMP_DIR);
		String userStatDir = conf.get(Config.UG_STAT_DIR);
		Path path = new Path(tempDir, userStatDir);
		DataLoader.LoadGroupMap(path, conf, userGroupMap);
	}
	
	@Override
	protected void map(LongWritable itemID, PAForItemWithGNWritable prefs, Context context) throws IOException,
			InterruptedException {

		int groupID = prefs.getItemGroupID();
		Statistic stat = new Statistic();
		for (Preference pref : prefs.getPreferenceArray()) {
			stat.IncrNumValue(getUserGroupID(pref.getUserID()), pref.getValue(), 1);
		}
		stat.AddID(itemID.get());
		log.info(stat.toString());
		context.write(new IntWritable(groupID), new StatisticWritable(stat));
	}
	
	private int getUserGroupID(long userID){
		for(int groupID : userGroupMap.keySet()){
			if(userGroupMap.get(groupID).contains(userID)){
				return groupID;
			}
		}
		return (int)(userID % 3 + 1);
	}
}
