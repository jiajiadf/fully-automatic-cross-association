package org.apache.mahout.cf.taste.hadoop.faca;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

import cn.edu.bjtu.cit.math.MathUtil;

public class InfoUtil {
	
	
	public static Map<Float, Long> getUserStatMap(List<PreferenceArray> prefList, FastIDSet itemIDs){
		Map<Float, Long> countMap = new HashMap<Float, Long>();
		for(PreferenceArray prefs : prefList){
			for(Preference pref : prefs){
				if(itemIDs.contains(pref.getItemID())){
					float value = pref.getValue();
					if(countMap.containsKey(value)){
						countMap.put(value, countMap.get(value) + 1);
					}
					else{
						countMap.put(value, 1l);
					}
				}
			}
		}
		return countMap;
	}
	

	/**
	 * @param stat		Statistic info of a user or item group
	 * @param groupID	Group ID
	 * @param IDs		All the IDs in item or user group
	 * @return
	 */
	public static double C(Statistic stat, int groupID, FastIDSet IDs){
		double c = 0.0;
		long na = stat.getNumIDs() * IDs.size();
		c = na * H(stat.getCountMap(groupID), na);
		return c;
	}
	
	public static double H(Map<Float, Long> map, long na){
		double entropy = 0.0, p = 0.0;
		for(Map.Entry<Float, Long> elem : map.entrySet()){
			p = P(map, na, elem.getKey());
			if(p != 0){
				entropy += p * MathUtil.log(p, 2);
			}
		}
		p = P(map, na, 0);
		if(p != 0){
			entropy += p * MathUtil.log(p, 2);
		}
		return (-1) * entropy;
		
	}
	
	public static double P1(Map<Float, Long> map, long na, float value) {
		double p = 0.0; 
		long numValues;
		if(value == 0){
			numValues = na - getNumOfTotalValues(map);
		}
		else{
			numValues = map.get(value);
		}
		p = (numValues + 0.5) / (na + 1);
		return p;
	}
	
	public static double P(Map<Float, Long> map, long na, float value) {
		double p = 0.0; 
		long numValues;
		if(value == 0){
			numValues = na - getNumOfTotalValues(map);
		}
		else{
			numValues = map.get(value);
		}
		p = (double)numValues / na;
		return p;
	}
	
	private static long getNumOfTotalValues(Map<Float, Long> map){
		long sum = 0;
		for(long value : map.values()){
			sum += value;
		}
		return sum;
	}
	
	
	public static void main(String[] args) {
		Statistic stat = new Statistic();
		FastIDSet ids = new FastIDSet(new long[]{1, 2, 3, 4, 5});
		stat.AddIDs(ids);
		
		
		stat.setNumValue(1, 2, 6);
		stat.setNumValue(1, 3, 4);
		stat.setNumValue(1, 4, 7);
		stat.setNumValue(1, 5, 4);
		
		
		Map.Entry<Integer, FastIDSet> entry = new Map.Entry<Integer, FastIDSet>() {

			@Override
			public Integer getKey() {
				return 1;
			}

			@Override
			public FastIDSet getValue() {
				FastIDSet set = new FastIDSet();
				set.add(101);
				set.add(102);
				set.add(103);
				set.add(104);
				set.add(105);
				set.add(106);
				set.add(107);
				return set;
			}
			
			@Override
			public FastIDSet setValue(FastIDSet value) {
				// TODO Auto-generated method stub
				return null;
			}
		};
		
		double entropy = 0.0;
		long bj = 7;
		entropy = InfoUtil.H(stat.getCountMap(entry.getKey()), stat.getNumTotalValues(entry.getKey()));
		
		System.out.println(entropy * bj);
	}
}
