package org.apache.mahout.cf.taste.hadoop.faca.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAFromUserWithGNWritable;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.impl.model.GenericUserPreferenceArray;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;

public class UserPrefsReducer extends Reducer<LongWritable, Text, LongWritable, PAFromUserWithGNWritable> {

	@Override
	protected void reduce(LongWritable userID, Iterable<Text> item_pref_strs, Context context) throws IOException,
			InterruptedException {
		List<Preference> list = new ArrayList<Preference>();
		for (Text itemPref : item_pref_strs) {
			String[] tokens = itemPref.toString().split(":");
			long itemID = Long.parseLong(tokens[0]);
			float pref = Float.parseFloat(tokens[1]);
			list.add(new GenericPreference(userID.get(), itemID, pref));
		}
		PreferenceArray pa = new GenericUserPreferenceArray(list);
		int userGroupID = (int)(userID.get() % 3 + 1);
		context.write(userID, new PAFromUserWithGNWritable(pa, userGroupID));
	}
}
