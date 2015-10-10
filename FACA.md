# Introduction #

This project is an implementation of a clustering algorithm based on paper [Fully Automatic Cross-associations](http://dl.acm.org/citation.cfm?id=1014052.1014064)(FACA) and [DisCo: Distributed Co-clustering with Map-Reduce](http://ieeexplore.ieee.org/xpl/login.jsp?tp=&arnumber=4781146&url=http%3A%2F%2Fieeexplore.ieee.org%2Fxpls%2Fabs_all.jsp%3Farnumber%3D4781146)(DisCo), this algorithm is the first step of my further research of recommender algorithm. FACA depends on [Mahout](http://mahout.apache.org/), so you should add Mahout into your classpath.

# Details #

## Implementation ##

There two major part of the clustering algorithm, (1)Split the group, (2)ReGroup, according to DisCo, algorithm of (2) can be parallel, but (1) can't.

### Steps ###
  1. Given a input file such as ml-100k's data file, faca parses it into an PreferenceArray in Mahout and calculates the statistic information of each cluster
  1. Find the row cluster with maximum entropy and split this row
  1. ReGroup
  1. Calculating the total cost, if it lower than before, go to step 2, else go to step 5
  1. Do the same thing but for column

I've implement two versions of faca, CrossAssociationSearch() is the algorithm in FACA and CrossAssociationSearch2() is a new version  which do the split and regroup for row and group alternately.

### Usage ###

Since this project is for recommender algorithm, the names of variable in it are related to user-item matrix. Here's some instruction of how to run faca.

### Input ###
Download the test dataset from http://www.grouplens.org/node/73, I use ml-100k and ml-1m dataset as my input and the result is user group and item group.

### Output ###
Get result in temp dir specified by the command line parameter --tempDir, another parameter --outputDir is not used but you should set it. In temp dir, there are a lot of sub folders, the group info are saved in userGroupStat and itemGroupStat, the files in them are Hadoop sequence file. You can read them using Hadoop API.

### Main class ###
The main class is ParallelFACAJob, it's a sub class of AbstractJob in Mahout

## Limitation and Future work ##
It's a long time since I write these code and I remember there are some mistakes in calculating the entropy and total cost, these issues will affect the correctness of FACA, the result got by some dataset make sense and some don't.
For such a complex algorithm like FACA, it is very different implement using Hadoop API, I think some high level framework based on hadoop such as [Cascading](http://www.cascading.org/), FlumeJava and [Crunch](http://incubator.apache.org/crunch/) are suitable for this use case, they will make you away from a lot of work which is unrelated to the algorithm such as intermediate file handling and cascading the MapReduce Job. You can focus on the algorithm if you use such framework.