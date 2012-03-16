package org.apache.mahout.cf.taste.hadoop.faca;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileToText {
	public static void main(String[] args) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		String parent = "src/main/resources/temp/userGroupStat";
		String map = "part-m-00000";
		String reduce = "part-r-00000";
		Path path_r = getPath(parent, reduce);
		Path path_m = getPath(parent, map);
		Path path = null;
		if(fs.exists(path_r)){
			path = path_r;
		}
		else{
			path = path_m;
		}
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, fs.getConf());
		Writable key = (Writable)ReflectionUtils.newInstance(reader.getKeyClass(), fs.getConf());
		Writable value = (Writable)ReflectionUtils.newInstance(reader.getValueClass(), fs.getConf());
		while(reader.next(key, value)){
			System.out.println(key.toString());
			System.out.println(value.toString());
		}
	}
	
	public static Path getPath(String parent, String path){
		return new Path(parent, path);
	}
}
