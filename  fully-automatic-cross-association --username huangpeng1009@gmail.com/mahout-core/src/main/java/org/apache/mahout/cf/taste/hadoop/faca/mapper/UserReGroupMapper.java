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
import org.apache.mahout.cf.taste.hadoop.faca.data.PAFromUserWithGNWritable;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;
import org.apache.mahout.cf.taste.hadoop.faca.data.StatisticWritable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.Preference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UserReGroupMapper extends Mapper<LongWritable, PAFromUserWithGNWritable, IntWritable, StatisticWritable>{
	
	private Logger log = LoggerFactory.getLogger(UserReGroupMapper.class);
	private Map<Integer, FastIDSet> itemGroupMap;
	private Map<Integer, Statistic> userStatMap;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Path pathToItemPrefs = new Path(context.getConfiguration().get(Config.TEMP_DIR),
										context.getConfiguration().get(Config.ITEM_PREFS_DIR));
		Path pathToRowGroupStat = new Path(context.getConfiguration().get(Config.TEMP_DIR),
										   context.getConfiguration().get(Config.UG_STAT_DIR));
		
		Configuration conf = context.getConfiguration();
		
		itemGroupMap = new HashMap<Integer, FastIDSet>();
		userStatMap = new HashMap<Integer, Statistic>();
		DataLoader.LoadGroupMap(pathToItemPrefs, conf, itemGroupMap);
		DataLoader.LoadStat(pathToRowGroupStat, conf, userStatMap);
	}
	
	@Override
	protected void map(LongWritable key, PAFromUserWithGNWritable value, Context context) throws IOException, InterruptedException {
		int oldGroupID = value.getUserGroupID();
		int groupID = oldGroupID;
		Statistic rowstat = userStatistic(value);
		double cost = getCost(rowstat, userStatMap.get(oldGroupID), itemGroupMap);
		for(Map.Entry<Integer, Statistic> entry : userStatMap.entrySet()){
			if(entry.getKey() != oldGroupID){
				Statistic groupstat = entry.getValue();
				groupstat.AddStat(rowstat);
				double curcost = getCost(rowstat, groupstat, itemGroupMap);
				if(curcost < cost){
					cost = curcost;
					groupID = entry.getKey();
				}
				groupstat.RemoveStat(rowstat);
			}
		}
		if(oldGroupID != groupID){
			log.info("User " + key.get() + ": " + oldGroupID + "------>" + groupID);
		}
		context.write(new IntWritable(groupID), new StatisticWritable(rowstat));
	}
	
	private Statistic userStatistic(PAFromUserWithGNWritable pw){
		Statistic stat = new Statistic();
		stat.AddID(pw.getPreferenceArray().getUserID(0));
		for(Preference pref : pw.getPreferenceArray()){
			stat.IncrNumValue(getItemGroupID(pref.getItemID()), pref.getValue(), 1);
		}
		return stat;
	}
	
	private int getItemGroupID(long itemID){
		for(int groupID : itemGroupMap.keySet()){
			if(itemGroupMap.get(groupID).contains(itemID)){
				return groupID;
			}
		}
		return 1;
	}
	
	private double getCost(Statistic rowStat, Statistic groupStat, Map<Integer, FastIDSet> gmap){
		double cost = 0.0;
		for(int j : gmap.keySet()){
			long na = groupStat.getNumIDs() * gmap.get(j).size();
			for(float u : rowStat.getCountMap(j).keySet()){
				cost += rowStat.getNumValues(j, u) * ((double)1 / InfoUtil.P1(groupStat.getCountMap(j), na, u));
			}
			cost += (gmap.get(j).size() - rowStat.getNumTotalValues(j)) * ((double)1 / InfoUtil.P1(groupStat.getCountMap(j), na, 0));
		}
		return cost;
	}
}
