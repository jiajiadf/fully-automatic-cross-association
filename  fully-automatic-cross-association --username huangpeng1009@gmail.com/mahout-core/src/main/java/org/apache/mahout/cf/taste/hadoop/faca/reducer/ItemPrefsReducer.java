package org.apache.mahout.cf.taste.hadoop.faca.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAForItemWithGNWritable;
import org.apache.mahout.cf.taste.impl.model.GenericItemPreferenceArray;
import org.apache.mahout.cf.taste.impl.model.GenericPreference;
import org.apache.mahout.cf.taste.model.Preference;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ItemPrefsReducer extends Reducer<LongWritable, Text, LongWritable, PAForItemWithGNWritable> {
	
	private Logger log = LoggerFactory.getLogger(ItemPrefsReducer.class);
	
	@Override
	protected void reduce(LongWritable itemID, Iterable<Text> user_pref_strs, Context context) throws IOException,
			InterruptedException {
		List<Preference> list = new ArrayList<Preference>();
		for (Text userPref : user_pref_strs) {
			String[] tokens = userPref.toString().split(":");
			long userID = Long.parseLong(tokens[0]);
			float pref = Float.parseFloat(tokens[1]);
			list.add(new GenericPreference(userID, itemID.get(), pref));
		}
		PreferenceArray pa = new GenericItemPreferenceArray(list);
		int itemGroupID = (int)(itemID.get() - 100) % 3 + 1;
		PAForItemWithGNWritable pw = new PAForItemWithGNWritable(pa, itemGroupID);
		context.write(itemID, pw);
		log.info(pw.toString());
	}
}
