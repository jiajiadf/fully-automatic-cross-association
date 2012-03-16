package org.apache.mahout.cf.taste.hadoop.faca.data;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.mahout.cf.taste.impl.common.FastIDSet;

public class FastIDSetSplit implements Iterable<FastIDSet>{
	
	private List<FastIDSet> list;
	private int groupID;
	private double gain;
	
	public FastIDSetSplit(List<FastIDSet> list, int groupID){
		this.list = list;
		this.groupID = groupID;
		gain = 0.0;
	}
	
	public FastIDSetSplit(){
		list = new ArrayList<FastIDSet>();
		groupID = 0;
		gain = 0.0;
	}
	
	public void Add(FastIDSet set){
		list.add(set);
	}
	
	public int getNumOfSplit(){
		return list.size();
	}
	
	public int getGroupID(){
		return groupID;
	}
	
	public void setGroupID(int value){
		groupID = value;
	}
	
	public double getGain(){
		return gain;
	}
	
	public void setGain(double value){
		gain = value;
	}
	
	public void setGroupList(List<FastIDSet> list){
		this.list = list;
	}
	
	public List<FastIDSet> getGroupList(){
		return list;
	}
	
	public boolean contains(long id, int idx){
		if(idx > 1){
			throw new IllegalArgumentException("Illegal index: " + idx + ", index in FastIDSetSplit must less than 2");
		}
		if(list.get(idx).contains(id)){
			return true;
		}
		else{
			return false;
		}
	}
	
	@Override
	public String toString(){
		return groupID + ": " + list.toString() + " gain: " + gain;
	}
	
	@Override
	public Iterator<FastIDSet> iterator() {
		return list.iterator();
	}
	
	@Override
	public FastIDSetSplit clone(){
		FastIDSetSplit ret = new FastIDSetSplit();
		ret.setGroupID(this.groupID);
		for(FastIDSet set : list){
			ret.Add(set.clone());
		}
		return ret;
	}

}
