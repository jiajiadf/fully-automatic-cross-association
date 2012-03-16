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
import org.apache.mahout.cf.taste.hadoop.faca.InfoUtil;
import org.apache.mahout.cf.taste.hadoop.faca.data.DataLoader;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAForItemWithGNWritable;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;
import org.apache.mahout.cf.taste.hadoop.faca.data.StatisticWritable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.Preference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemReGroupMapper extends Mapper<LongWritable, PAForItemWithGNWritable, IntWritable, StatisticWritable>{
	
	private Logger log = LoggerFactory.getLogger(ItemReGroupMapper.class);
	private Map<Integer, FastIDSet> userGroupMap;
	private Map<Integer, Statistic> itemStatMap;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Path pathToUserPrefs = new Path(context.getConfiguration().get(Config.TEMP_DIR),
										context.getConfiguration().get(Config.USER_PREFS_DIR));
		Path pathToColGroupStat = new Path(context.getConfiguration().get(Config.TEMP_DIR),
										   context.getConfiguration().get(Config.IG_STAT_DIR));
		
		Configuration conf = context.getConfiguration();
		
		userGroupMap = new HashMap<Integer, FastIDSet>();
		itemStatMap = new HashMap<Integer, Statistic>();
		DataLoader.LoadGroupMap(pathToUserPrefs, conf, userGroupMap);
		DataLoader.LoadStat(pathToColGroupStat, conf, itemStatMap);
	}
	
	@Override
	protected void map(LongWritable key, PAForItemWithGNWritable value, Context context) throws IOException, InterruptedException {
		int oldGroupID = value.getItemGroupID();
		int groupID = oldGroupID;
		Statistic colstat = colStatistic(value);
		double cost = getCost(colstat, itemStatMap.get(oldGroupID), userGroupMap);
		for(Map.Entry<Integer, Statistic> entry : itemStatMap.entrySet()){
			if(entry.getKey() != oldGroupID){
				Statistic groupstat = entry.getValue();
				groupstat.AddStat(colstat);
				double curcost = getCost(colstat, groupstat, userGroupMap);
				if(curcost < cost){
					cost = curcost;
					groupID = entry.getKey();
				}
				groupstat.RemoveStat(colstat);
			}
		}
		if(groupID != oldGroupID){
			log.info("Item " + key.get() + ": " + oldGroupID + "------>" + groupID);
		}
		context.write(new IntWritable(groupID), new StatisticWritable(colstat));
	}
	
	private Statistic colStatistic(PAForItemWithGNWritable pw){
		Statistic stat = new Statistic();
		stat.AddID(pw.getPreferenceArray().getItemID(0));
		for(Preference pref : pw.getPreferenceArray()){
			stat.IncrNumValue(getUserGroupID(pref.getUserID()), pref.getValue(), 1);
		}
		return stat;
	}
	
	private int getUserGroupID(long userID){
		for(int groupID : userGroupMap.keySet()){
			if(userGroupMap.get(groupID).contains(userID)){
				return groupID;
			}
		}
		return 1;
	}
	
	private double getCost(Statistic colStat, Statistic groupStat, Map<Integer, FastIDSet> gmap){
		double cost = 0.0;
		for(int j : gmap.keySet()){
			long na = groupStat.getNumIDs() * gmap.get(j).size();
			for(float u : colStat.getCountMap(j).keySet()){
				cost += colStat.getNumValues(j, u) * ((double)1 / InfoUtil.P1(groupStat.getCountMap(j), na, u));
			}
			cost += (gmap.get(j).size() - colStat.getNumTotalValues(j)) * ((double)1 / InfoUtil.P1(groupStat.getCountMap(j), na, 0));
		}
		return cost;
	}
}
