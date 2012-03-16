package org.apache.mahout.cf.taste.hadoop.faca.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class StatisticWritable implements Writable{
	
	private Statistic stat;
	
	public StatisticWritable(Statistic stat){
		this.stat = stat;
	}
	
	public StatisticWritable(){
		stat = new Statistic();
	}
	
	public Statistic get(){
		return stat;
	}
	
	public void set(Statistic stat){
		this.stat = stat;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		
		int i;
		stat = new Statistic();
		
		long numIDs = in.readLong();
		for(i = 0; i < numIDs; i++){
			stat.AddID(in.readLong());
		}
		
		int outerSize = in.readInt();
		for(i = 0; i < outerSize; i++){
			int groupID = in.readInt();
			int inSize = in.readInt();
			Map<Float, Long> map = new HashMap<Float, Long>();
			for(int j = 0; j < inSize; j++){
				map.put(in.readFloat(), in.readLong());
			}
			stat.getGroupCountMap().put(groupID, map);
		}
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(stat.getNumIDs());
		for(long id : stat.getIDs()){
			out.writeLong(id);
		}
		
		int size = stat.getGroupCountMap().entrySet().size();
		out.writeInt(size);
		for(Map.Entry<Integer, Map<Float, Long>> entry : stat.getGroupCountMap().entrySet()){
			int groupID = entry.getKey();
			out.writeInt(groupID);
			Map<Float, Long> map = entry.getValue();
			size = map.entrySet().size();
			out.writeInt(size);
			for(Map.Entry<Float, Long> ele : map.entrySet()){
				float key = ele.getKey();
				out.writeFloat(key);
				long value = ele.getValue();
				out.writeLong(value);
			}
		}
	}
	
	@Override
	public String toString(){
		return stat.toString();
	}
				
}
