package org.apache.mahout.cf.taste.hadoop.faca.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

public class PAFromUserWithGNWritable implements Writable{
	
	private PreferenceArray pa;
	private int userGroupID;
	
	public PAFromUserWithGNWritable(PreferenceArray value, int gn){
		pa = value;
		userGroupID = gn;
	}
	
	public PAFromUserWithGNWritable(){
		
	}
	
	public PreferenceArray getPreferenceArray() {
		return pa;
	}

	public void setPreferenceArray(PreferenceArray pa) {
		this.pa = pa;
	}
	
	public int getUserGroupID(){
		return userGroupID;
	}
	
	public void setUserGroupID(int value){
		userGroupID = value;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		List<Preference> list = new ArrayList<Preference>();
		long userID = in.readLong();
		int gn = in.readInt();
		userGroupID = gn;
		int len = in.readInt();
		for(int i = 0; i < len; i++){
			long itemID = in.readLong();
			float value = in.readFloat();
			Preference pref = new GenericPreference(userID, itemID, value);
			list.add(pref);
		}
		pa = new GenericUserPreferenceArray(list);
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(pa.getUserID(0));
		out.writeInt(userGroupID);
		out.writeInt(pa.length());
		for(int i = 0; i < pa.length(); i++){
			Preference pref = pa.get(i);
			out.writeLong(pref.getItemID());
			out.writeFloat(pref.getValue());
		}
	}
	
	@Override
	public String toString(){
		StringBuffer sb = new StringBuffer();
		sb.append("User GroupID: " + userGroupID + "\n");
		sb.append(pa.toString() + "\n");
		return sb.toString();
	}
	
	
	
}
