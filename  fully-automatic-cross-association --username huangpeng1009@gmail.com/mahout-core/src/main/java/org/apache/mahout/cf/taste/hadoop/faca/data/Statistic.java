package org.apache.mahout.cf.taste.hadoop.faca.data;

import java.util.HashMap;
import java.util.Map;

import org.apache.mahout.cf.taste.impl.common.FastIDSet;

public class Statistic {
	private Map<Integer, Map<Float, Long>> groupCountMap;
	private FastIDSet IDs;
	
	public Statistic(){
		groupCountMap = new HashMap<Integer, Map<Float, Long>>();
		IDs = new FastIDSet();
	}
	
	public Statistic(Map<Integer, Map<Float, Long>> map, FastIDSet set){
		groupCountMap = map;
		IDs = set;
	}
	
	public Map<Integer, Map<Float, Long>> getGroupCountMap(){
		return groupCountMap;
	}
	
	public long getNumIDs(){
		return IDs.size();
	}
	
	public FastIDSet getIDs(){
		return IDs;
	}
	
	public void AddID(long ID){
		IDs.add(ID);
	}
	
	public void AddIDs(FastIDSet IDs){
		this.IDs.addAll(IDs);
	}
	
	public void RemoveID(long ID){
		IDs.remove(ID);
	}
	
	public void RemoveIDs(FastIDSet IDs){
		this.IDs.removeAll(IDs);
	}
	
	/**
	 * @param groupID
	 * @return Count map of groupID
	 */
	public Map<Float, Long> getCountMap(int groupID){
		if(!getGroupCountMap().containsKey(groupID)){
			return new HashMap<Float, Long>();
		}
		return getGroupCountMap().get(groupID);
	}
	
	/**
	 * @param groupID 
	 * @param key pref value
	 * @return Number of values which equal key in groupID 
	 */
	public long getNumValues(int groupID, float key){
		Map<Float, Long> countMap = getCountMap(groupID);
		if(!countMap.containsKey(key)){
			return 0;
		}
		return countMap.get(key);
	}
	
	/**
	 * @param groupID
	 * @return Number of rating values in groupID
	 */
	public long getNumTotalValues(int groupID){
		Map<Float, Long> map = getCountMap(groupID);
		long sum = 0;
		for(Map.Entry<Float, Long> entry : map.entrySet()){
			sum += entry.getValue();
		}
		return sum;
	}
	
	/**
	 * Increase the number of values which equal key in groupID
	 * @param groupID
	 * @param key
	 * @param num
	 */
	public void IncrNumValue(int groupID, float key, long num){
		if(!groupCountMap.containsKey(groupID)){
			Map<Float, Long> map = new HashMap<Float, Long>();
			map.put(key, num);
			groupCountMap.put(groupID, map);
		}
		else{
			Map<Float, Long> map = groupCountMap.get(groupID);
			if(map.containsKey(key)){
				map.put(key, map.get(key) + num);
			}
			else{
				map.put(key, num);
			}
		}
	}
	
	/**
	 * Decrease the number of values which equal key in groupID
	 * @param groupID
	 * @param key
	 * @param num
	 */
	public void DecrNumValue(int groupID, float key, long num){
		Map<Float, Long> countMap = getCountMap(groupID);
		if(!countMap.containsKey(key)){
			throw new IllegalArgumentException("Group " + groupID + " doesn't contain value " + key);
		}
		if(num > countMap.get(key)){
			throw new IllegalArgumentException("Value in group " + groupID + "(" + countMap.get(key) + ") is less than " + num);
		}
		long value = countMap.get(key) - num;
		if(value == 0){
			countMap.remove(key);
			if(getGroupCountMap().get(groupID).entrySet().size() == 0){
				getGroupCountMap().remove(groupID);
			}
		}
		else{
			countMap.put(key, countMap.get(key) - num);
		}
	}
	
	/**
	 * Set the number of values in groupID
	 * @param groupID
	 * @param key		Value want to set
	 * @param value		Number of values(key)
	 */
	public void setNumValue(int groupID, float key, long value){
		if(groupCountMap.containsKey(groupID)){
			groupCountMap.get(groupID).put(key, value);
		}
		else{
			Map<Float, Long> map = new HashMap<Float, Long>();
			map.put(key, value);
			groupCountMap.put(groupID, map);
		}
	}
	
	/**
	 * Add a Statistic into this one
	 * @param stat
	 */
	public void AddStat(Statistic stat){
		for(Map.Entry<Integer, Map<Float, Long>> entry : stat.getGroupCountMap().entrySet()){
			int groupID = entry.getKey();
			Map<Float, Long> map = entry.getValue();
			if(!getGroupCountMap().containsKey(groupID)){
				getGroupCountMap().put(groupID, new HashMap<Float, Long>());
				for(float key : map.keySet()){
					getGroupCountMap().get(groupID).put(key, map.get(key));
				}
			}
			else{
				for(Map.Entry<Float, Long> ele : map.entrySet()){
					IncrNumValue(groupID, ele.getKey(), ele.getValue());
				}
			}
		}
		AddIDs(stat.getIDs());
	}
	
	/**
	 * Remove a Statistic from this one
	 * @param stat
	 */
	public void RemoveStat(Statistic stat){
		for(Map.Entry<Integer, Map<Float, Long>> entry : stat.getGroupCountMap().entrySet()){
			int groupID = entry.getKey();
			Map<Float, Long> map = entry.getValue();
			for(Map.Entry<Float, Long> ele : map.entrySet()){
				DecrNumValue(groupID, ele.getKey(), ele.getValue());
			}
		}
		RemoveIDs(stat.getIDs());
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append(IDs.toString() + "\n");
		sb.append(groupCountMap.toString() + "\n");
		return sb.toString();
	}
}
