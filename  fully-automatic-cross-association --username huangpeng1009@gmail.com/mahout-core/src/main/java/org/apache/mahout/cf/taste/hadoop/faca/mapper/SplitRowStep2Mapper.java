package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.faca.Config;
import org.apache.mahout.cf.taste.hadoop.faca.InfoUtil;
import org.apache.mahout.cf.taste.hadoop.faca.data.DataLoader;
import org.apache.mahout.cf.taste.hadoop.faca.data.FastIDSetSplit;
import org.apache.mahout.cf.taste.hadoop.faca.data.SplitWritable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SplitRowStep2Mapper extends Mapper<IntWritable, NullWritable, IntWritable, SplitWritable>{
	
	private Logger log = LoggerFactory.getLogger(SplitRowStep2Mapper.class);
	
	private int groupID = 0;
	private List<PreferenceArray> userPrefsList;
	private Map<Integer, FastIDSet> itemGroupMap;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		userPrefsList = new ArrayList<PreferenceArray>();
		itemGroupMap = new HashMap<Integer, FastIDSet>();
		groupID = context.getConfiguration().getInt(Config.GROUP_ID, 1);
		String tempDir = context.getConfiguration().get(Config.TEMP_DIR);
		String userPrefsDir = context.getConfiguration().get(Config.USER_PREFS_DIR);
		String itemPrefsDir = context.getConfiguration().get(Config.ITEM_PREFS_DIR);
		DataLoader.LoadUserGroupPrefs(new Path(tempDir, userPrefsDir), context.getConfiguration(), groupID, userPrefsList);
		DataLoader.LoadGroupMap(new Path(tempDir, itemPrefsDir), context.getConfiguration(), itemGroupMap);
	}

	@Override
	protected void map(IntWritable key, NullWritable value, Context context) throws IOException, InterruptedException {
		if(userPrefsList.size() == 1 || key.get() == userPrefsList.size()){
			FastIDSetSplit split = new FastIDSetSplit();
			split.Add(getUserIDs(userPrefsList));
			split.setGroupID(groupID);
			SplitWritable sw = new SplitWritable(split);
			context.write(new IntWritable(groupID), sw);
			log.info("Not split " + sw.toString());
		}
		else{
			SplitWritable sw = split(userPrefsList, itemGroupMap, key.get(), groupID);
			log.info(sw.toString());
			context.write(new IntWritable(groupID), sw);
		}
		
	}
	
	private SplitWritable split(List<PreferenceArray> userPrefsList, Map<Integer, FastIDSet> map, int pos, int groupID){
		double gain = 0.0;
		SplitWritable sw = new SplitWritable();
		List<PreferenceArray> slice1 = new ArrayList<PreferenceArray>();
		List<PreferenceArray> slice2 = new ArrayList<PreferenceArray>();
		for(int i = 0; i < userPrefsList.size(); i++){
			if(i < pos){
				slice1.add(userPrefsList.get(i));
			}
			else{
				slice2.add(userPrefsList.get(i));
			}
		}
		FastIDSetSplit split = new FastIDSetSplit();
		split.Add(getUserIDs(slice1));
		split.Add(getUserIDs(slice2));
		split.setGroupID(groupID);
		sw.set(split);
		
		double p1 = (double)slice1.size() / userPrefsList.size();
		double p2 = (double)slice2.size() / userPrefsList.size();
		
		gain = RowEntropy(userPrefsList, map) - (p1 * RowEntropy(slice1, map) + p2 * RowEntropy(slice2, map));
		sw.get().setGain(gain);
		return sw;
	}
	
	private FastIDSet getUserIDs(List<PreferenceArray> list){
		FastIDSet set = new FastIDSet();
		for(PreferenceArray prefs : list){
			set.add(prefs.getUserID(0));
		}
		return set;
	}
	
	private double RowEntropy(List<PreferenceArray> list, Map<Integer, FastIDSet> map){
		double ret = 0.0;
		for(FastIDSet itemIDs : map.values()){
			Map<Float, Long> stat = InfoUtil.getUserStatMap(list, itemIDs);
			int na = list.size() * itemIDs.size();
			double h = InfoUtil.H(stat, na);
			ret += h;
		}
		return ret;
	}
}
