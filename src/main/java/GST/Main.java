package GST;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;
import scala.Tuple2;

/**
 * Created by Liang on 16-11-9.
 * This is the enter point of program
 */
public class Main {
    private static int FmSelector(int fileSize) {
        if (fileSize < 5000000) //5000 1000
            return 50000;
        else if (fileSize < 30000000)//50000 1000
            return 150000;
        else if (fileSize < 50000000)//500000 20
            return 140000;
        else if (fileSize < 75000000)//50000 5000
            return 110000;
        else if (fileSize < 80000000)//1000000 100
            return 90000;
        else //500000 1000
            return 200000;
    }

    //"ClassRegistrator" must be public
    public static class ClassRegistrator implements KryoRegistrator {
        public void registerClasses(Kryo kryo) {
            kryo.register(ERA.L_B.class, new FieldSerializer(kryo, ERA.L_B.class));
            kryo.register(ERA.TreeNode.class, new FieldSerializer(kryo, ERA.TreeNode.class));
            kryo.register(ERA.class, new FieldSerializer(kryo, ERA.class));
        }
    }

    public static String readFile(String url, char terminator) throws IOException {
        Path path = new Path(url);
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", url);//hdfs://master:9000
        FileSystem fileSystem = FileSystem.get(conf);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            sb.append(line);
        }
        sb.append(terminator);
        return sb.toString();
    }


    public static List<String> listFiles(String url) throws IOException {
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

    public static void main(String[] args) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        final String inputURL = args[0];
        final String outputURL = args[1];
        String param = "default";
        if (args.length == 3)
            param = args[2];
        SparkConf conf = new SparkConf().
                setAppName(new Path(inputURL).getName() + " Fm:" + param);
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", ClassRegistrator.class.getName());
        conf.set("spark.kryoserializer.buffer.max", "2047");
        final ERA era = new ERA();
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();//终结符:文件名
        List<String> pathList = listFiles(inputURL);
        List<String> S = new ArrayList<String>();
        //开始读取文本文件
        //key filename value content

        for (String path : pathList) {
            char terminator = era.nextTerminator();
            String content = readFile(path, terminator);
            S.add(content);
            terminatorFilename.put(terminator, new Path(path).getName());
        }

        int lengthForAll = 0;
        for (String s : S)
            lengthForAll += s.length();
//        int Fm = FmSelector(lengthForAll);
        int Fm;
        if (param.equals("default"))
            Fm = FmSelector(lengthForAll);
        else
            Fm = Integer.valueOf(param);
        System.out.println("read:" + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        Set<Character> alphabet = ERA.getAlphabet(S);//扫描串获得字母表
        System.out.println("scan alphabet:" + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        Set<Set<String>> setOfVirtualTrees = era.verticalPartitioningTest(S, alphabet, Fm);//开始垂直分区
        System.out.println("vertical partition:" + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();
        System.gc();
        //分配任务
        final Broadcast<List<String>> broadcastStringList = sc.broadcast(S);
        final Broadcast<Map<Character, String>> broadcasterTerminatorFilename = sc.broadcast(terminatorFilename);
        JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<Set<String>>(setOfVirtualTrees), setOfVirtualTrees.size());
        JavaRDD<Set<String>> tmp = vtRDD.map(new Function<Set<String>, Set<String>>() {
            public Set<String> call(Set<String> strings) throws Exception {
                Set<String> res = new HashSet<String>();
                List<String> mainString = broadcastStringList.value();
                Map<Character, String> terminatorFilename = broadcasterTerminatorFilename.value();
                for (String pi : strings) {
                    ERA.L_B lb = era.subTreePrepare(mainString, pi);
                    ERA.TreeNode root = era.buildSubTree(mainString, lb);
                    era.splitSubTree(mainString, pi, root);
                    era.traverseTree(mainString, root, terminatorFilename, res);
                }
                return res;
            }
        });
        JavaRDD<String> resultRDD = tmp.flatMap(new FlatMapFunction<Set<String>, String>() {
            public Iterable<String> call(Set<String> strings) throws Exception {
                return strings;
            }
        });
        resultRDD.saveAsTextFile(outputURL);
        System.out.println("other procedure:" + (System.currentTimeMillis() - start));
    }
}