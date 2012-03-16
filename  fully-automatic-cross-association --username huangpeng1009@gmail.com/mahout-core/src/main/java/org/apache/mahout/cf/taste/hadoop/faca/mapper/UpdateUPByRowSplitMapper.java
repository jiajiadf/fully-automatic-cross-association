package org.apache.mahout.cf.taste.hadoop.faca.mapper;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.cf.taste.hadoop.faca.Config;
import org.apache.mahout.cf.taste.hadoop.faca.data.DataLoader;
import org.apache.mahout.cf.taste.hadoop.faca.data.FastIDSetSplit;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAFromUserWithGNWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateUPByRowSplitMapper extends Mapper<LongWritable, PAFromUserWithGNWritable, LongWritable, PAFromUserWithGNWritable>{
	
	private Logger log = LoggerFactory.getLogger(UpdateUPByRowSplitMapper.class);
	private FastIDSetSplit split;
	private Path pathToSplit;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		String tempDir = context.getConfiguration().get(Config.TEMP_DIR);
		pathToSplit = new Path(tempDir, context.getConfiguration().get(Config.SPLIT_DIR));
		split = new FastIDSetSplit();
		DataLoader.LoadFastIDSetSpilt(pathToSplit, context.getConfiguration(), split);
	}
	
	@Override
	protected void map(LongWritable key, PAFromUserWithGNWritable value, Context context) throws IOException, InterruptedException {
		long userID = key.get();
		if(split.getNumOfSplit() == 1){
			log.info(value.toString());
			context.write(key, value);
		}
		else{
			if(!split.contains(userID, 0) && value.getUserGroupID() >= split.getGroupID()){
				value.setUserGroupID(value.getUserGroupID() + 1);
				log.info(value.toString());
				context.write(key, value);
			}
			else{
				log.info(value.toString());
				context.write(key, value);
			}
		}
	}
	
	@Override
	protected void cleanup(Context context) throws IOException {
		Path path = new Path(context.getConfiguration().get("mapred.input.dir"));
		DataLoader.deleteFile(path, context.getConfiguration());
	}
}
