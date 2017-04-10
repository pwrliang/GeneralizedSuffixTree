package GST;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.hadoop.fs.Path;
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

/**
 * Created by Liang on 16-11-9.
 * This is the enter point of program
 */
public class Main {
    private static int FmSelector(int fileSize) {
        if (fileSize < 70000000) //other unknown small data set
            return 1000000;
        else if (fileSize < 80000000)//1000000 100
            return 1500000;
        else if (fileSize < 210000000)//2M_100
            return 1400000;
        else if (fileSize < 240000000)//500000_1000
            return 1100000;
        else if (fileSize < 420000000)//20M_20
            return 1900000;
        else //unknown big data set
            return 2500000;
    }

    //"ClassRegistrator" must be public
    public static class ClassRegistrator implements KryoRegistrator {
        public void registerClasses(Kryo kryo) {
            kryo.register(ERA.L_B.class, new FieldSerializer(kryo, ERA.L_B.class));
            kryo.register(ERA.TreeNode.class, new FieldSerializer(kryo, ERA.TreeNode.class));
            kryo.register(ERA.class, new FieldSerializer(kryo, ERA.class));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final String inputURL = args[0];
        final String outputURL = args[1];
        final int Fm = Integer.parseInt(args[2]);
        SparkConf conf = new SparkConf().
                setAppName("GST"+new Path(inputURL).getName()+" Fm:"+Fm);

        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", ClassRegistrator.class.getName());
        conf.set("spark.kryoserializer.buffer.max", "2047");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final Map<Character, String> terminatorFilename = new HashMap<>();//终结符:文件名
        final List<String> S = new ArrayList<>();

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
//        int Fm = FmSelector(lengthForAll);
        Set<Character> alphabet = ERA.getAlphabet(S);//扫描串获得字母表
        Set<Set<String>> setOfVirtualTrees = era.verticalPartitioning(S, alphabet, Fm);//开始垂直分区
        //分配任务
        final Broadcast<List<String>> broadcastStringList = sc.broadcast(S);
        final Broadcast<Map<Character, String>> broadcasterTerminatorFilename = sc.broadcast(terminatorFilename);

        JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<>(setOfVirtualTrees), setOfVirtualTrees.size());
        JavaRDD<Set<String>> tmp = vtRDD.map(new Function<Set<String>, Set<String>>() {
            public Set<String> call(Set<String> prefixSet) throws Exception {
                Set<String> result = new HashSet<>();
                List<String> mainString = broadcastStringList.value();
                Map<Character, String> terminatorFilename = broadcasterTerminatorFilename.value();
                Map<String, List<int[]>> prefixLoc = era.getPrefixLoc(mainString, prefixSet);//一次寻找前缀集合中所有前缀的位置
                for (String prefix : prefixLoc.keySet()) {
                    ERA.L_B lb = era.subTreePrepare(S, prefix, prefixLoc.get(prefix));
                    ERA.TreeNode root = era.buildSubTree(mainString, lb);
                    era.splitSubTree(mainString, prefix, root);
                    era.traverseTree(mainString, root, terminatorFilename, result);
                }
                return result;
            }
        });
        JavaRDD<String> resultRDD = tmp.flatMap(new FlatMapFunction<Set<String>, String>() {
            public Iterable<String> call(Set<String> strings) throws Exception {
                return strings;
            }
        });
        resultRDD.saveAsTextFile(outputURL);
    }
}