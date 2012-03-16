package org.apache.mahout.cf.taste.hadoop.faca.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;

public class SplitWritable implements Writable {
	
	private FastIDSetSplit split;

	
	public SplitWritable(FastIDSetSplit split){
		this.split = split;
	}
	
	public SplitWritable(){
		split = new FastIDSetSplit();
	}
	
	public FastIDSetSplit get(){
		return split;
	}
	
	public void set(FastIDSetSplit value){
		split = value;
	}
	
	
	@Override
	public String toString(){
		return split.toString();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(split.getGain());
		out.writeInt(split.getGroupID());
		out.writeInt(split.getNumOfSplit());
		for(FastIDSet set : split){
			out.writeInt(set.size());
			for(long id : set){
				out.writeLong(id);
			}
		}
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		double gain = in.readDouble();
		int groupID = in.readInt();
		List<FastIDSet> list = new ArrayList<FastIDSet>();
		int size = in.readInt();
		for(int i = 0; i < size; i++){
			FastIDSet set = new FastIDSet();
			int setSize = in.readInt();
			for(int j = 0; j < setSize; j++){
				set.add(in.readLong());
			}
			list.add(set);
		}
		split = new FastIDSetSplit(list, groupID);
		split.setGain(gain);
	}

}
