package GST;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.sun.corba.se.spi.presentation.rmi.IDLNameTranslator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;
import scala.Char;
import scala.Tuple2;

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
    private static String readFile(String url) throws IOException {
        Path path = new Path(url);
        URI uri = path.toUri();
        String hdfsPath = String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsPath);//hdfs://master:9000
        FileSystem fileSystem = FileSystem.get(conf);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        StringBuilder sb = new StringBuilder();
        char[] cbuf = new char[4096];
        int len;
        while ((len = bufferedReader.read(cbuf)) != -1) {
            sb.append(cbuf, 0, len);
        }
        bufferedReader.close();
        return sb.toString();
    }

    private static List<String> listFiles(String url) throws IOException {
        Path path = new Path(url);
        URI uri = path.toUri();
        String hdfsPath = String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsPath);

        FileSystem fileSystem = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
        List<String> pathList = new ArrayList<String>();
        while (files.hasNext()) {
            LocatedFileStatus file = files.next();
            pathList.add(file.getPath().toString());
        }
        return pathList;
    }

    public static class ClassRegistrator implements KryoRegistrator {
        public void registerClasses(Kryo kryo) {
            kryo.register(ERA.L_B.class, new FieldSerializer(kryo, ERA.class));
            kryo.register(ERA.TreeNode.class, new FieldSerializer(kryo, ERA.TreeNode.class));
            kryo.register(ERA.class, new FieldSerializer(kryo, ERA.class));
        }
    }

    static int[] getParameter(String datasetName) {
        //int[0]=Fm int[1]=range
        if (datasetName.contains("5000 1000")) {
            return new int[]{50000, 1000};
        } else if (datasetName.contains("50000 1000")) {
            return new int[]{50000, 4000};
        } else if (datasetName.contains("50000 5000")) {
            return new int[]{60000, 5000};
        }
        return new int[]{50000, 5000};
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        SparkConf sparkConf = new SparkConf().
                setAppName("Generalized Suffix Tree");
//        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        sparkConf.set("spark.kryo.registrator", ClassRegistrator.class.getName());
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final String inputURL = args[0];
        final String outputURL = args[1];
        final String tmpURL = args[2];
        final int Fm = Integer.parseInt(args[3]);
        final int ELASTIC_RANGE = Integer.parseInt(args[4]);
        sc.setCheckpointDir(tmpURL);

        //开始读取文本文件
//        List<String> pathList = listFiles(inputURL);
        Map<Character, String> terminatorFilename = new HashMap<Character, String>();//终结符:文件名
        List<String> S = new ArrayList<String>();
        final ERA era = new ERA(ELASTIC_RANGE);
//        for (String filename : pathList) {
//            String content = readFile(filename);
//            Character terminator = era.nextTerminator();
//            S.add(content + terminator);
//            terminatorFilename.put(terminator, filename.substring(filename.lastIndexOf('/') + 1));
//        }
        //key filename value content
        JavaPairRDD<String, String> inputData = sc.wholeTextFiles(inputURL);//read whole folder
        Map<String, String> dataSet = inputData.collectAsMap();//key file path, value content
        for (String path : dataSet.keySet()) {
            String filename = new Path(path).getName();
            Character terminator = era.nextTerminator();
            S.add(dataSet.get(path) + terminator);//append terminator to the end of text
            terminatorFilename.put(terminator, filename);
        }

        Set<Character> alphabet = ERA.getAlphabet(S);//扫描串获得字母表
        System.out.println("==================Start Vertical Partition=======================");
        Set<Set<String>> setOfVirtualTrees = era.verticalPartitioning(S, alphabet, Fm);//开始垂直分区
        System.out.println("==================Vertical Partition Finished setOfVirtualTrees:" + setOfVirtualTrees.size() + "================");
        //分配任务
        final Broadcast<List<String>> broadcastStringList = sc.broadcast(S);
        final Broadcast<Map<Character, String>> broadcasterTerminatorFilename = sc.broadcast(terminatorFilename);
        JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<Set<String>>(setOfVirtualTrees));
        JavaPairRDD<List<String>, List<ERA.L_B>> subtreePrepared = vtRDD.mapToPair(new PairFunction<Set<String>, List<String>, List<ERA.L_B>>() {
            public Tuple2<List<String>, List<ERA.L_B>> call(Set<String> strings) throws Exception {
                List<String> piList = new ArrayList<String>(strings);//不能直接用Set，因为不保证有序
                List<ERA.L_B> LBList = new ArrayList<ERA.L_B>();
                for (String pi : piList)
                    LBList.add(era.subTreePrepare(broadcastStringList.getValue(), pi));
                return new Tuple2<List<String>, List<ERA.L_B>>(piList, LBList);
            }
        });
        subtreePrepared.checkpoint();
        System.out.println("==================SubTree Prepare Finished=======================");
        JavaRDD<String> resultsRDD = subtreePrepared.map(new Function<Tuple2<List<String>, List<ERA.L_B>>, String>() {
            public String call(Tuple2<List<String>, List<ERA.L_B>> listListTuple2) throws Exception {
                List<String> piList = listListTuple2._1;//pi列表
                List<ERA.L_B> L_B_List = listListTuple2._2;//L,B数组
                StringBuilder partialResult = new StringBuilder();//piList形成子树遍历的结果
                for (int i = 0; i < piList.size(); i++) {
                    String pi = piList.get(i);
                    ERA.L_B lb = L_B_List.get(i);
                    ERA.TreeNode root = era.buildSubTree(broadcastStringList.getValue(), lb);
                    era.splitSubTree(broadcastStringList.getValue(), pi, root);
                    partialResult.append(era.traverseTree(broadcastStringList.getValue(), root, broadcasterTerminatorFilename.getValue()));
                }
                //去掉多余的回车？？
                return partialResult.deleteCharAt(partialResult.length() - 1).toString();
            }
        });
        resultsRDD.checkpoint();
        resultsRDD.saveAsTextFile(outputURL);
        System.out.println("=====================Tasks Done============");
    }
}