# SeeDB: efficient data-driven visualization recommendations to support visual analytics #

 We implemented the algorithm based on the definition in Section 2 of the paper, Shared-based Optimization (through query rewriting) in Section 4.1, and Pruning-based Optimization (using Hoeffding-Serfling inequality) in Section 4.2.
 In evaluation, We usec the census data set. Set the user-specified query to include the married people, and the reference query to include unmarried people. 
 Used the K-L Divergence as the utility measure.

 The Project Consists of Code and Data folders 
 
 Code : 
 Preprocess.py  : Data preprocessing  
 
 Main.py  :Share-based optimization  
 Pruning.py  :Pruning based optimization  
 Utils.py  :helper functions  
 

 Data :
[Census dataset](https://archive.ics.uci.edu/dataset/20/census+income)
