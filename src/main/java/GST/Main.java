package GST;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;

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
    public static class ClassRegistrator implements KryoRegistrator {
        public void registerClasses(Kryo kryo) {
            kryo.register(ERA.L_B.class, new FieldSerializer(kryo, ERA.class));
            kryo.register(ERA.TreeNode.class, new FieldSerializer(kryo, ERA.TreeNode.class));
            kryo.register(ERA.class, new FieldSerializer(kryo, ERA.class));
        }
    }

    static int FmSelector(int fileSize){
        if(fileSize<5000000) //5000 1000
            return 50000;
        else if(fileSize<30000000)//50000 1000
            return 40000;
        else if(fileSize<50000000)//500000 20
            return 700000;
        else if(fileSize<75000000)//50000 5000
            return 75000;
        else if(fileSize<78000000)//1000000 100
            return 70000;
        else //500000 1000
            return 150000;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final String inputURL = args[0];
        final String outputURL = args[1];
        SparkConf sparkConf = new SparkConf().
                setAppName(new Path(inputURL).getName());
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", ClassRegistrator.class.getName());
        sparkConf.set("spark.kryoserializer.buffer.max","2047");
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

        int lenthForAll = 0;
        for (String s : S)
            lenthForAll += s.length();
        final int PARTITIONS = sc.defaultParallelism() * 4;
        int Fm = FmSelector(lenthForAll);

        System.out.println("Fm:" + Fm + " AllLen:" + lenthForAll);
        Set<Character> alphabet = ERA.getAlphabet(S);//扫描串获得字母表
        long start = System.currentTimeMillis();
        System.out.println("==================Start Vertical Partition version 1.16-1=======================");
        Set<Set<String>> setOfVirtualTrees = era.verticalPartitioning(S, alphabet, Fm);//开始垂直分区
        System.out.println("==================Vertical Partition Finished time:" + (System.currentTimeMillis() - start) / 1000 + " setOfVirtualTrees:" + setOfVirtualTrees.size() + "================");
        //分配任务
        final Broadcast<List<String>> broadcastStringList = sc.broadcast(S);
        final Broadcast<Map<Character, String>> broadcasterTerminatorFilename = sc.broadcast(terminatorFilename);
        final JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<Set<String>>(setOfVirtualTrees), PARTITIONS);
        JavaRDD<Map<String, ERA.L_B>> subtreePrepared = vtRDD.map(new Function<Set<String>, Map<String, ERA.L_B>>() {
            public Map<String, ERA.L_B> call(Set<String> input) throws Exception {
                List<String> mainString = broadcastStringList.value();
                Map<String, ERA.L_B> piLB = new HashMap<String, ERA.L_B>();
                for (String pi : input)
                    piLB.put(pi, era.subTreePrepare(mainString, pi));
                return piLB;
            }
        });
//      //building subTrees & traverse
        JavaRDD<String> resultsRDD = subtreePrepared.map(new Function<Map<String, ERA.L_B>, String>() {
            public String call(Map<String, ERA.L_B> input) throws Exception {
                List<String> mainString = broadcastStringList.value();
                Map<Character, String> terminatorFilename = broadcasterTerminatorFilename.value();
                StringBuilder partialResult = new StringBuilder();
                for (String pi : input.keySet()) {
                    ERA.L_B lb = input.get(pi);
                    ERA.TreeNode root = era.buildSubTree(mainString, lb);
                    era.splitSubTree(mainString, pi, root);
                    partialResult.append(era.traverseTree(mainString, root, terminatorFilename));
                }
                return partialResult.deleteCharAt(partialResult.length() - 1).toString();
            }
        });
        resultsRDD.saveAsTextFile(outputURL);
        System.out.println("=====================Tasks Done============");
    }
}