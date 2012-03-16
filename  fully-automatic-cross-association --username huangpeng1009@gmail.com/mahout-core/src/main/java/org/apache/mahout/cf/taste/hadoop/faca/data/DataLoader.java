package org.apache.mahout.cf.taste.hadoop.faca.data;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.lucene.util.IOUtils;
import org.apache.mahout.cf.taste.impl.common.FastIDSet;
import org.apache.mahout.cf.taste.model.PreferenceArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataLoader {
	
	private static Logger log = LoggerFactory.getLogger(DataLoader.class);
	
	/**
	 * Load item group information
	 * @param path
	 * @param conf
	 * @param map
	 * @throws IOException
	 */
	public static void LoadGroupMap(Path path, Configuration conf, Map<Integer, FastIDSet> map) throws IOException{
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Reader reader = null;
		try {
			if(fs.exists(path)){
				log.info(path.getName() + "doesn't exist");
				return;
			}
			FileStatus[] files = fs.listStatus(path);
			for(FileStatus file : files){
				if(file.isDir() || !file.getPath().getName().matches("part-[A-Za-z0-9]-\\d+")){
					continue;
				}
				reader = new SequenceFile.Reader(fs, file.getPath(), fs.getConf());
				IntWritable key = new IntWritable();
				StatisticWritable value = new StatisticWritable();
				while (reader.next(key, value)) {
					Statistic stat = value.get();
					int groupID = key.get();
					if(map.containsKey(groupID)){
						map.get(groupID).addAll(stat.getIDs());
					}
					else{
						map.put(groupID, stat.getIDs());
					}
				}
			}
		} finally {
			IOUtils.close(reader);
		}
	}
	
	/**
	 * Load user preferences in one group
	 * @param path
	 * @param conf
	 * @param groupID
	 * @param map
	 * @throws IOException
	 */
	public static void LoadUserGroupPrefs(Path path, Configuration conf, int groupID, List<PreferenceArray> list) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(path.getName()), conf);
		SequenceFile.Reader reader = null;
		try {
			if(fs.exists(path)){
				log.info(path.getName() + "doesn't exist");
				return;
			}
			FileStatus[] files = fs.listStatus(path);
			for(FileStatus file : files){
				if(file.isDir() || !file.getPath().getName().matches("part-[A-Za-z0-9]-\\d+")){
					continue;
				}
				reader = new SequenceFile.Reader(fs, file.getPath(), fs.getConf());
				LongWritable key = new LongWritable();
				PAFromUserWithGNWritable value = new PAFromUserWithGNWritable();
				while (reader.next(key, value)) {
					if(groupID == value.getUserGroupID()){
						list.add(value.getPreferenceArray());
					}
				}
			}
		} finally {
			IOUtils.close(reader);
		}
	}
	
	public static void LoadFastIDSetSpilt(Path path, Configuration conf, FastIDSetSplit split) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(path.getName()), conf);
		SequenceFile.Reader reader = null;
		try {
			if(fs.exists(path)){
				log.info(path.getName() + "doesn't exist");
				return;
			}
			FileStatus[] files = fs.listStatus(path);
			for (FileStatus file : files) {
				if (file.isDir() || !file.getPath().getName().matches("part-[A-Za-z0-9]-\\d+")) {
					continue;
				}
				reader = new SequenceFile.Reader(fs, file.getPath(), fs.getConf());
				IntWritable key = new IntWritable();
				SplitWritable value = new SplitWritable();
				while (reader.next(key, value)) {
					split.setGroupList(value.get().getGroupList());
					split.setGain(value.get().getGain());
					split.setGroupID(value.get().getGroupID());
				}
			}

		} finally {
			IOUtils.close(reader);
		}
	}
	
	public static void LoadStat(Path path, Configuration conf, Map<Integer, Statistic> map) throws IOException{
		FileSystem fs = FileSystem.get(URI.create(path.getName()), conf);
		SequenceFile.Reader reader = null;
		if(!fs.exists(path)){
			log.info(path.getName() + "doesn't exist");
			return;
		}
		try {
			FileStatus[] files = fs.listStatus(path);
			for (FileStatus file : files) {
				if (file.isDir() || !file.getPath().getName().matches("part-[A-Za-z0-9]-\\d+")) {
					continue;
				}
				reader = new SequenceFile.Reader(fs, file.getPath(), fs.getConf());
				IntWritable key = new IntWritable();
				StatisticWritable value = new StatisticWritable();
				while (reader.next(key, value)) {
					Statistic stat = value.get();
					int groupID = key.get();
					map.put(groupID, stat);
				}
			}
		} finally {
			IOUtils.close(reader);
		}
	}
	
	public static boolean deleteFile(Path path, Configuration conf) {
		FileSystem fs;
		boolean ret = false;
		try {
			fs = FileSystem.get(URI.create(path.getName()), new Configuration());
			if (fs.exists(path)) {
				ret = fs.delete(path, true);
				fs.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}
}
