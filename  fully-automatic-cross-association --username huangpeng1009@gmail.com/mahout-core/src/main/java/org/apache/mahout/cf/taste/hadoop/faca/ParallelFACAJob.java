package org.apache.mahout.cf.taste.hadoop.faca;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.cf.taste.hadoop.faca.data.DataLoader;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAForItemWithGNWritable;
import org.apache.mahout.cf.taste.hadoop.faca.data.PAFromUserWithGNWritable;
import org.apache.mahout.cf.taste.hadoop.faca.data.SplitWritable;
import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;
import org.apache.mahout.cf.taste.hadoop.faca.data.StatisticWritable;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.CopyMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.GetItemGroupIDMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.GetUserGroupIDMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.ItemGroupStatMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.ItemPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.ItemReGroupMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.SplitRowStep1Mapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.SplitRowStep2Mapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.UpdateIPByColStatMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.UpdateIPByRowSplitMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.UpdateUPByRowSplitMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.UpdateUPByRowStatMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.UserGroupStatMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.UserPrefsMapper;
import org.apache.mahout.cf.taste.hadoop.faca.mapper.UserReGroupMapper;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.ColGroupStatCombiner;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.ColReGroupReducer;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.GetItemGroupIDReducer;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.GetUserGroupIDReducer;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.ItemGroupStatReducer;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.ItemPrefsReducer;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.MaxEntropyColReducer;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.MaxEntropyRowReducer;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.UserReGroupReducer;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.SplitRowReducer;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.UserGroupStatCombiner;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.UserGroupStatReducer;
import org.apache.mahout.cf.taste.hadoop.faca.reducer.UserPrefsReducer;
import org.apache.mahout.common.AbstractJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParallelFACAJob extends AbstractJob {
	
	private Logger log = LoggerFactory.getLogger(ParallelFACAJob.class);
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new ParallelFACAJob(), args);
	}
	
	private String tempDir;
	private String outputDir;
	private int k = 1, l = 1;
	@Override
	public int run(String[] args) throws Exception {
		
		addInputOption();
		addOutputOption();
		
		Map<String,String> parsedArgs = parseArguments(args);
	    if (parsedArgs == null) {
	      return -1;
	    }
	    
	    tempDir = parsedArgs.get("--tempDir");
	    outputDir = parsedArgs.get("--outputDir");
	    
	    cleanup();
	    //Get user preferences
	    InitUserPrefs();
	    //Get item preferences
	    InitItemPrefs();
	    ReGroup(3, 3);
	    //Search for cross association
	    //CrossAssociationSearch();
	    //int groupID = findMaxRowEntropyGroupID();
	    //int groupID = findMaxColEntropyGroupID();
	    //int r_split = splitUserGroup(groupID);
	    //int c_split = splitItemGroup(groupID);
	    /*if(r_split != 0){
	    	updateUserPrefs();
	    	k++;
	    	ReGroup(k, l);
	    }*/
/*	    if(c_split != 0){
	    	updateItemPrefs();
	    	l++;
	    	ReGroup(k, l);
	    }*/
		return 0;
	}
	
	private void cleanup() throws IOException{
		DataLoader.deleteFile(new Path(tempDir), new Configuration());
	}
	
	private void CrossAssociationSearch(){
		/*double codeLen = Double.MAX_VALUE;
		double curLen = totalCodeLen();
		try {
			//row search
			while(curLen < codeLen){
				codeLen = curLen;
				int groupID = findMaxRowEntropyGroupID();
				splitUserGroup(groupID);
				updateUserPrefs();
				ReGroup(k, l);
				updateUserPrefs();
				curLen = totalCodeLen();
			}
			
			codeLen = Double.MAX_VALUE;
			while(curLen < codeLen){
				codeLen = curLen;
				int groupID = findMaxColEntropyGroupID();
				int l_split = splitItemGroup(groupID);
				ReGroup(k, l);
				updateItemPrefs();
				curLen = totalCodeLen();
			}
			//column search
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}
	
	private void ReGroup(int k, int l) throws IOException, ClassNotFoundException, InterruptedException{
		double totalCost = Double.MAX_VALUE;
		double curCost = totalCodeLen(); 
		if(totalCost > curCost){
			log.info("totalCost = {}, curCost = {}", totalCost, curCost);
			totalCost = curCost;
			if(k != 1){
				UserReGroup();
			}
/*			if(l != 1){
				ItemReGroup();
			}*/
			curCost = totalCodeLen();
			log.info("curCost = {}", curCost);
		}
	}
	
	private void UserReGroup() throws IOException, ClassNotFoundException, InterruptedException{
		//Get new user group stat
		Job userReGroupJob = prepareJob(pathToUserPrefs(), pathToUserGroupStatU(),
	            SequenceFileInputFormat.class, UserReGroupMapper.class, IntWritable.class,
	            StatisticWritable.class, UserReGroupReducer.class, IntWritable.class,
	            StatisticWritable.class, SequenceFileOutputFormat.class);
		userReGroupJob.getConfiguration().set(Config.TEMP_DIR, tempDir);
		userReGroupJob.getConfiguration().set(Config.ITEM_PREFS_DIR, pathToItemPrefs().getName());
		userReGroupJob.getConfiguration().set(Config.UG_STAT_DIR, pathToUserGroupStat().getName());
		userReGroupJob.waitForCompletion(true);
		
		//Update user group in user prefs
		Job userUpdateJob = prepareJob(pathToUserPrefs(), pathToUserPrefsU(),
	            SequenceFileInputFormat.class, UpdateUPByRowStatMapper.class, LongWritable.class,
	            PAFromUserWithGNWritable.class, Reducer.class, LongWritable.class,
	            PAFromUserWithGNWritable.class, SequenceFileOutputFormat.class);
		userUpdateJob.getConfiguration().set(Config.TEMP_DIR, tempDir);
		userUpdateJob.getConfiguration().set(Config.UG_STAT_DIR, pathToUserGroupStatU().getName());
		userUpdateJob.setNumReduceTasks(0);
		userUpdateJob.waitForCompletion(true);
		
		Configuration conf = new Configuration();
		copy(pathToUserPrefsU(), pathToUserPrefs(), LongWritable.class, PAFromUserWithGNWritable.class);
		//copy(pathToUserGroupStatU(), pathToUserGroupStat(), IntWritable.class, StatisticWritable.class);
		//DataLoader.deleteFile(pathToUserGroupStatU(), conf);
		
		//Get new item group stat
		DataLoader.deleteFile(pathToItemGroupStat(), conf);
		ItemGroupStat(pathToItemPrefs(), pathToItemGroupStat(), pathToUserGroupStatU());
		copy(pathToUserGroupStatU(), pathToUserGroupStat(), IntWritable.class, StatisticWritable.class);
	}
	
	
	private void ItemReGroup() throws IOException, ClassNotFoundException, InterruptedException{
		Job itemReGroupJob = prepareJob(pathToItemPrefs(), pathToItemGroupStatU(),
	            SequenceFileInputFormat.class, ItemReGroupMapper.class, IntWritable.class,
	            StatisticWritable.class, ColReGroupReducer.class, IntWritable.class,
	            StatisticWritable.class, SequenceFileOutputFormat.class);
		itemReGroupJob.getConfiguration().set(Config.TEMP_DIR, tempDir);
		itemReGroupJob.getConfiguration().set(Config.USER_PREFS_DIR, pathToUserPrefs().getName());
		itemReGroupJob.getConfiguration().set(Config.IG_STAT_DIR, pathToItemGroupStat().getName());
		itemReGroupJob.waitForCompletion(true);
		
		Job itemUpdateJob = prepareJob(pathToItemPrefs(), pathToItemPrefsU(),
	            SequenceFileInputFormat.class, UpdateIPByColStatMapper.class, LongWritable.class,
	            PAForItemWithGNWritable.class, Reducer.class, LongWritable.class,
	            PAForItemWithGNWritable.class, SequenceFileOutputFormat.class);
		itemUpdateJob.getConfiguration().set(Config.TEMP_DIR, tempDir);
		itemUpdateJob.getConfiguration().set(Config.IG_STAT_DIR, pathToItemGroupStatU().getName());
		itemUpdateJob.setNumReduceTasks(0);
		itemUpdateJob.waitForCompletion(true);
		
		Configuration conf = new Configuration();
		copy(pathToItemPrefsU(), pathToItemPrefs(), LongWritable.class, PAForItemWithGNWritable.class);
		//copy(pathToItemGroupStatU(), pathToItemGroupStat(), IntWritable.class, StatisticWritable.class);
		//DataLoader.deleteFile(pathToItemGroupStatU(), conf);
		
		DataLoader.deleteFile(pathToUserGroupStat(), conf);
		UserGroupStat(pathToUserPrefs(), pathToUserGroupStat(), pathToItemGroupStatU());
		copy(pathToItemGroupStatU(), pathToItemGroupStat(), IntWritable.class, StatisticWritable.class);
	}
	
	private void InitUserPrefs() throws Exception{
	    //get user ratings
	    Job userRatings = prepareJob(getInputPath(), pathToUserPrefs(),
	            TextInputFormat.class, UserPrefsMapper.class, LongWritable.class,
	            Text.class, UserPrefsReducer.class, LongWritable.class,
	            PAFromUserWithGNWritable.class, SequenceFileOutputFormat.class);
	    userRatings.waitForCompletion(true);
	    //get user statistic
	    UserGroupStat(pathToUserPrefs(), pathToUserGroupStat(), pathToItemGroupStat());
	}
	
	private void InitItemPrefs() throws Exception{
	    //get item ratings
	    Job itemRatings = prepareJob(getInputPath(), pathToItemPrefs(),
	    		TextInputFormat.class, ItemPrefsMapper.class, LongWritable.class,
	    		Text.class, ItemPrefsReducer.class, LongWritable.class,
	    		PAForItemWithGNWritable.class, SequenceFileOutputFormat.class);
	    itemRatings.waitForCompletion(true);
	    
	    ItemGroupStat(pathToItemPrefs(), pathToItemGroupStat(), pathToUserGroupStat());
	}
	
	private void UserGroupStat(Path input, Path output, Path itemStatPath) throws IOException, ClassNotFoundException, InterruptedException{
		Job userGroupStatJob = prepareJob(input, output,
	    		SequenceFileInputFormat.class, UserGroupStatMapper.class, IntWritable.class,
	    		StatisticWritable.class, UserGroupStatReducer.class, IntWritable.class,
	    		StatisticWritable.class, SequenceFileOutputFormat.class);
	    userGroupStatJob.setCombinerClass(UserGroupStatCombiner.class);
	    userGroupStatJob.getConfiguration().set(Config.TEMP_DIR, tempDir);
	    userGroupStatJob.getConfiguration().set(Config.IG_STAT_DIR, itemStatPath.getName());
	    userGroupStatJob.waitForCompletion(true);
	}
	
	private void ItemGroupStat(Path input, Path output, Path userStatPath) throws IOException, ClassNotFoundException, InterruptedException{
		Job itemGroupStatJob = prepareJob(input, output,
	    		SequenceFileInputFormat.class, ItemGroupStatMapper.class, IntWritable.class,
	    		StatisticWritable.class, ItemGroupStatReducer.class, IntWritable.class,
	    		StatisticWritable.class, SequenceFileOutputFormat.class);
	    itemGroupStatJob.setCombinerClass(ColGroupStatCombiner.class);
	    itemGroupStatJob.getConfiguration().set(Config.TEMP_DIR, tempDir);
	    itemGroupStatJob.getConfiguration().set(Config.UG_STAT_DIR, userStatPath.getName());
	    itemGroupStatJob.waitForCompletion(true);
	}
	
	private void copy(Path input, Path output, Class<? extends Writable> keyClass, Class<? extends Writable> valueClass)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		DataLoader.deleteFile(output, conf);
		Job copyJob = prepareJob(input, output, SequenceFileInputFormat.class, CopyMapper.class, keyClass, valueClass, Reducer.class,
				keyClass, valueClass, SequenceFileOutputFormat.class);
		copyJob.setNumReduceTasks(0);
		copyJob.waitForCompletion(true);
		DataLoader.deleteFile(input, conf);
	}
	
	private int findMaxRowEntropyGroupID() throws Exception{
	    DataLoader.deleteFile(new Path(tempDir, pathToMaxEntropyResult().getName()), new Configuration());
	    
	    Job maxEntropyRowJob = prepareJob(pathToUserGroupStat(), pathToMaxEntropyResult(),
	    		SequenceFileInputFormat.class, CopyMapper.class, IntWritable.class,
	    		StatisticWritable.class, MaxEntropyRowReducer.class, IntWritable.class,
	    		DoubleWritable.class, SequenceFileOutputFormat.class);	    
	    maxEntropyRowJob.getConfiguration().set(Config.ITEM_PREFS_DIR, pathToItemPrefs().getName());
	    maxEntropyRowJob.waitForCompletion(true);
	    
	    DataLoader.deleteFile(new Path(tempDir, pathToNull().getName()), new Configuration());
	    
	    Job getIDJob = prepareJob(pathToMaxEntropyResult(), pathToNull(),
	    		SequenceFileInputFormat.class, GetUserGroupIDMapper.class, IntWritable.class,
	    		Text.class, GetUserGroupIDReducer.class, NullWritable.class, NullWritable.class,
	    		SequenceFileOutputFormat.class);
	    getIDJob.waitForCompletion(true);
	    int groupID = (int)getIDJob.getCounters().getGroup(Config.INFO).findCounter(Config.MAX_ENTROPY_ROW_ID).getValue();
	    log.info("Max entropy user groupID is " + groupID);
	    return groupID;
	}
	
	private int findMaxColEntropyGroupID() throws Exception{
		
	    
	    DataLoader.deleteFile(new Path(tempDir, pathToMaxEntropyResult().getName()), new Configuration());
	    
	    Job maxEntropyColJob = prepareJob(pathToItemGroupStat(), pathToMaxEntropyResult(),
	    		SequenceFileInputFormat.class, Mapper.class, IntWritable.class,
	    		StatisticWritable.class, MaxEntropyColReducer.class, IntWritable.class,
	    		DoubleWritable.class, SequenceFileOutputFormat.class);	    
	    maxEntropyColJob.getConfiguration().set(Config.USER_PREFS_DIR, pathToUserPrefs().getName());
	    maxEntropyColJob.waitForCompletion(true);
	    
	    DataLoader.deleteFile(new Path(tempDir, pathToNull().getName()), new Configuration());
	    
	    Job getIDJob = prepareJob(pathToMaxEntropyResult(), pathToNull(),
	    		SequenceFileInputFormat.class, GetItemGroupIDMapper.class, IntWritable.class,
	    		Text.class, GetItemGroupIDReducer.class, NullWritable.class, NullWritable.class,
	    		SequenceFileOutputFormat.class);
	    getIDJob.waitForCompletion(true);
	    int groupID = (int)getIDJob.getCounters().getGroup(Config.INFO).findCounter(Config.MAX_ENTROPY_COL_ID).getValue();
	    log.info("Max entropy item groupID is " + groupID);
	    return groupID;
		
	}
	
	private int splitUserGroup(int groupID) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		DataLoader.deleteFile(pathToSplitStep1(), conf);
		DataLoader.deleteFile(pathToSplitStep2(), conf);
		
		Job step1Job = prepareJob(pathToUserPrefs(), pathToSplitStep1(),
	    		SequenceFileInputFormat.class, SplitRowStep1Mapper.class, IntWritable.class,
	    		NullWritable.class, Reducer.class, IntWritable.class, NullWritable.class,
	    		SequenceFileOutputFormat.class);
		step1Job.getConfiguration().setInt(Config.GROUP_ID, groupID);
		step1Job.setNumReduceTasks(0);
		step1Job.waitForCompletion(true);
		
		Job step2Job = prepareJob(pathToSplitStep1(), pathToSplitStep2(),
	    		SequenceFileInputFormat.class, SplitRowStep2Mapper.class, IntWritable.class,
	    		SplitWritable.class, SplitRowReducer.class, IntWritable.class, SplitWritable.class,
	    		SequenceFileOutputFormat.class);
		step2Job.getConfiguration().setInt(Config.GROUP_ID, groupID);
		step2Job.getConfiguration().set(Config.USER_PREFS_DIR, pathToUserPrefs().getName());
		step2Job.waitForCompletion(true);
		
		int result = (int)step2Job.getCounters().findCounter(Config.INFO, Config.IS_SPLIT).getValue();
		return result;
	}
	
	private int splitItemGroup(int groupID){
		return 0;
	}
	
	/**
	 * Update 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	private void update() throws IOException, ClassNotFoundException, InterruptedException {
		Job updateUserPrefsJob = prepareJob(pathToUserPrefs(), pathToUserPrefsU(), SequenceFileInputFormat.class,
				UpdateUPByRowSplitMapper.class, LongWritable.class, PAFromUserWithGNWritable.class, Reducer.class,
				LongWritable.class, PAFromUserWithGNWritable.class, SequenceFileOutputFormat.class);
		updateUserPrefsJob.getConfiguration().set(Config.SPLIT_DIR, pathToSplitStep2().getName());
		updateUserPrefsJob.setNumReduceTasks(0);
		updateUserPrefsJob.waitForCompletion(true);
		
		Job updateItemPrefsJob = prepareJob(pathToItemPrefs(), pathToItemPrefsU(), SequenceFileInputFormat.class,
				UpdateIPByRowSplitMapper.class, LongWritable.class, PAForItemWithGNWritable.class, Reducer.class,
				LongWritable.class, PAForItemWithGNWritable.class, SequenceFileOutputFormat.class);
		updateItemPrefsJob.getConfiguration().set(Config.SPLIT_DIR, pathToSplitStep2().getName());
		updateItemPrefsJob.setNumReduceTasks(0);
		updateItemPrefsJob.waitForCompletion(true);
		
		Configuration conf = new Configuration();
		DataLoader.deleteFile(pathToUserGroupStat(), conf);
		DataLoader.deleteFile(pathToItemGroupStat(), conf);
		
		//UserGroupStat(pathToUserPrefsU(), pathToUserGroupStat());
		//ItemGroupStat(pathToItemPrefsU(), pathToItemGroupStat());
	}
	
	private double totalCodeLen() throws IOException{
		
		Map<Integer, Statistic> userGroupStat = new HashMap<Integer, Statistic>();
		Map<Integer, Statistic> itemGroupStat = new HashMap<Integer, Statistic>();
		
		DataLoader.LoadStat(pathToUserGroupStat(), new Configuration(), userGroupStat);
		DataLoader.LoadStat(pathToItemGroupStat(), new Configuration(), itemGroupStat);
		
		return new TotalCostCalculator(userGroupStat, itemGroupStat).getTotalCost();
		
	}
	
	
	
	//------------------------------------------Working Dir---------------------------------------------------
	private Path pathToUserPrefs() throws IOException {
		Path path = new Path(tempDir, "userPrefs");
		return path;
	}
	
	private Path pathToItemPrefs(){
		return new Path(tempDir, "itemPrefs");
	}
	
	private Path pathToUserGroupStat(){
		return new Path(tempDir, "userGroupStat");
	}
	
	private Path pathToItemGroupStat(){
		return new Path(tempDir, "itemGroupStat");
	}
	
	private Path pathToUserGroupStatU(){
		return new Path(tempDir, "userGroupStatU");
	}
	
	private Path pathToItemGroupStatU(){
		return new Path(tempDir, "itemGroupStatU");
	}
	
	private Path pathToMaxEntropyResult(){
		return new Path(tempDir, "IDWithEntropys");
	}
	
	private Path pathToNull(){
		return new Path(tempDir, "NullResult");
	}
	
	private Path pathToUserPrefsU(){
		return new Path(tempDir, "userPrefsU");
	}
	
	private Path pathToItemPrefsU(){
		return new Path(tempDir, "itemPrefsU");
	}
	
	private Path pathToSplitStep1(){
		return new Path(tempDir, "splitStep1");
	}
	
	private Path pathToSplitStep2(){
		return new Path(tempDir, "splitStep2");
	}
}
