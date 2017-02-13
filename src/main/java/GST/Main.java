package GST;

import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

//垂直分区
//Fm     range  time
//5000 1000 data set
//102400 1000   2.6min
//102400 100   8.1min
//102400 400   3.9min
//102400 5000  3.1min
//102400 2000  2.5min
//102400 1500  2.7min
//102400 800   2.7min
//50000 1000   1.7min
//10000 1000   1.4min
//60000 1000   1.4min
//30000 1000   1.2min
//50000 1000 data set
//50000 1000  37min
//60000 1000  43min
//40000 1000  80+min
//50000 2000  21min
//50000 3000  17min
//50000 4000  14min

/**
 * Created by Liang on 16-11-9.
 * This is the enter point of program
 */
public class Main {
    static int FmSelector(int fileSize) {
        if (fileSize < 5000000) //5000 1000
            return 30000;
        else if (fileSize < 30000000)//50000 1000
            return 40000;
        else if (fileSize < 50000000)//500000 20
            return 70000;
        else if (fileSize < 80000000)//50000 5000
            return 75000;
        else //500000 1000
            return 150000;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final String inputURL = args[0];
        final String outputURL = args[1];
        SparkConf sparkConf = new SparkConf().
                setAppName(new Path(inputURL).getName());
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();//终结符:文件名
        final List<String> S = new ArrayList<String>();

        //开始读取文本文件
        //key filename value content
        JavaPairRDD<String, String> inputData = sc.wholeTextFiles(inputURL);//read whole folder
        Map<String, String> dataSet = inputData.collectAsMap();//key file path, value content
        final ERA era = new ERA();
        for (String path : dataSet.keySet()) {
            String filename = new Path(path).getName();
            Character terminator = era.nextTerminator();
            S.add(dataSet.get(path) + terminator);//append terminator to the end of text
            terminatorFilename.put(terminator, filename);
        }

        int lengthForAll = 0;
        for (String s : S)
            lengthForAll += s.length();
        final int PARTITIONS = sc.defaultParallelism() * 4;
        int Fm = FmSelector(lengthForAll);

        Set<Character> alphabet = era.getAlphabet(S);
        System.out.println("==================Start Vertical Partition=======================");
        Set<Set<String>> setOfVirtualTrees = era.verticalPartitioning(S, alphabet, Fm);
        System.out.println("==================Vertical Partition Finished setOfVirtualTrees:" + setOfVirtualTrees.size() + "================");
        //分配任务
        JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<Set<String>>(setOfVirtualTrees), PARTITIONS);
        final Broadcast<List<String>> broadcastS = sc.broadcast(S);
        JavaRDD<ERA> works = vtRDD.map(new Function<Set<String>, ERA>() {
            public ERA call(Set<String> v1) throws Exception {
                List<String> stringList = broadcastS.value();
                return new ERA(stringList, v1, terminatorFilename);
            }
        });
//      执行任务
        System.out.println("=====================Start Tasks============");
        JavaRDD<String> result = works.map(new Function<ERA, String>() {
            public String call(ERA v1) throws Exception {
                return v1.work();
            }
        });
        result.saveAsTextFile(outputURL);
        System.out.println("=====================Tasks Done============");
    }
}
