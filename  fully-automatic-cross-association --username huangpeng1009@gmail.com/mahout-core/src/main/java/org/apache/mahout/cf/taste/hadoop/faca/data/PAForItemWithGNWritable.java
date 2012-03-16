package org.apache.mahout.cf.taste.hadoop.faca.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.cf.taste.impl.model.GenericItemPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
public class PAForItemWithGNWritable implements Writable {
	
	private PreferenceArray pa;
	private int itemGroupID;
	
	public PAForItemWithGNWritable(PreferenceArray value, int gn){
		pa = value;
		itemGroupID = gn;
	}
	
	public PAForItemWithGNWritable(){
		
	}
	
	public PreferenceArray getPreferenceArray() {
		return pa;
	}

	public void setPreferenceArray(PreferenceArray pa) {
		this.pa = pa;
	}
	
	public int getItemGroupID(){
		return itemGroupID;
	}
	
	public void setItemGroupID(int value){
		itemGroupID = value;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(pa.getItemID(0));
		out.writeInt(itemGroupID);
		out.writeInt(pa.length());
		for(int i = 0; i < pa.length(); i++){
			Preference pref = pa.get(i);
			out.writeLong(pref.getUserID());
			out.writeFloat(pref.getValue());
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		List<Preference> list = new ArrayList<Preference>();
		long itemID = in.readLong();
		int gn = in.readInt();
		itemGroupID = gn;
		int len = in.readInt();
		for(int i = 0; i < len; i++){
			long userID = in.readLong();
			float value = in.readFloat();
			Preference pref = new GenericPreference(userID, itemID, value);
			list.add(pref);
		}
		pa = new GenericItemPreferenceArray(list);
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("Item GroupID: " + itemGroupID + "\n");
		sb.append(pa.toString() + "\n");
		return sb.toString();
	}

}
