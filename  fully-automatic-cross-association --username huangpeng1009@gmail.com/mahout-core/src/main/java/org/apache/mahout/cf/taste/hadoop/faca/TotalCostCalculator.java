package org.apache.mahout.cf.taste.hadoop.faca;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.mahout.cf.taste.hadoop.faca.data.Statistic;

import cn.edu.bjtu.cit.math.MathUtil;

public class TotalCostCalculator {
	private Map<Integer, Statistic> userGroupStat;
	private Map<Integer, Statistic> itemGroupStat;
	
	public TotalCostCalculator(Map<Integer, Statistic> uMap, Map<Integer, Statistic> iMap){
		userGroupStat = uMap;
		itemGroupStat = iMap;
	}
	
	public Map<Integer, Statistic> getUserGroupStat(){
		return userGroupStat;
	}
	
	public Map<Integer, Statistic> getItemGroupStat(){
		return itemGroupStat;
	}
	
	public double getTotalCost(){
		double cost = 0.0;
		int k = userGroupStat.entrySet().size();
		int l = itemGroupStat.entrySet().size();
		cost = MathUtil.log(k, 2) + MathUtil.log(l, 2);
		
		Integer[] array = userGroupStat.keySet().toArray(new Integer[]{});
		for(int i = 1; i <= k - 1; i++){
			int ai_ = ai_ORbj_(array[i - 1], userGroupStat);
			cost += Math.ceil(MathUtil.log((double)ai_, 2));
		}
		
		array = itemGroupStat.keySet().toArray((new Integer[]{}));
		for(int j = 1; j <= l - 1; j++){
			int bj_ = ai_ORbj_(array[j - 1], itemGroupStat);
			cost += Math.ceil(MathUtil.log((double)bj_, 2));
		}
		
		for(int i : userGroupStat.keySet()){
			int ai = (int)userGroupStat.get(i).getNumIDs();
			for(int j : itemGroupStat.keySet()){
				int bj = (int)itemGroupStat.get(j).getNumIDs();
				cost += Math.ceil(MathUtil.log((double)(ai * bj + 1), 2));
			}
		}
		
		for(int i : userGroupStat.keySet()){
			for(int j : itemGroupStat.keySet()){
				cost += InfoUtil.C(userGroupStat.get(i), j, itemGroupStat.get(j).getIDs());
			}
		}
		
		return cost;
	}
	
	private List<Map.Entry<Integer, Statistic>> getOrderedList(Map<Integer, Statistic> map){
		List<Map.Entry<Integer, Statistic>> list = new LinkedList<Map.Entry<Integer, Statistic>>();
		for(Map.Entry<Integer, Statistic> entry : map.entrySet()){
			if(list.isEmpty()){
				list.add(entry);
			}
			else{
				int i;
				for(i = 0; i < list.size(); i++){
					if(list.get(i).getValue().getNumIDs() < entry.getValue().getNumIDs()){
						list.add(i, entry);
						break;
					}
				}
				if(i == list.size()){
					list.add(entry);
				}
			}
		}
		return list;
	}
	
	private int ai_ORbj_(int iOrj, Map<Integer, Statistic> map){
		int result = 0;
		List<Map.Entry<Integer, Statistic>> list = getOrderedList(map);
		int kOrl = list.size();
		for(int idx = iOrj; idx <= kOrl; idx++){
			result += list.get(idx - 1).getValue().getNumIDs();
		}
		result = result - kOrl + iOrj;
		return result;
	}
	
}
