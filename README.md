# Generalized Suffix Tree
## Introduction
This project is the implementation of [ERA: Efficient Serial and Parallel Suffix Tree Construction for Very Long Strings](http://www.vldb.org/pvldb/vol5/p049_essammansour_vldb2012.pdf) algorithm.
We optimized the vertical partition algorithm for best parallelism.
This project is also a problem in [The third term Cloud Computing Contest](https://cloud.seu.edu.cn/contest/)
[[Sildes](https://github.com/pwrliang/GeneralizedSuffixTree/raw/master/docs/slides.pptx)]
## Usage
This project build by maven, just open the project in Intellij Idea and compile&package into jar.
 
**e.g.** spark-submit --master spark://master:7077 --class GST.Main --executor-memory 6G --driver-memory 20G suffixtree-1.0-SNAPSHOT.jar "hdfs://master:9000/input/5000 1000" "hdfs://master:9000/output/5000 1000"

## Experiments
The code write in Java and run on Apache Spark. We tested on 20GB memory 48 cores cluster, for 400MB gene dataset this program build suffix tree and save result only consumed 4.4 min. 
<img src="https://raw.githubusercontent.com/pwrliang/GeneralizedSuffixTree/master/docs/fig.png" alt="" /><br />
**Note:**
AAA_BBB means, dataset is composed by 'BBB' strings, the longest string no more than 'AAA' characters.
