package GST;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.FieldSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.serializer.KryoRegistrator;
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
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            sb.append(line);
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
            kryo.register(SlavesWorks.L_B.class, new FieldSerializer(kryo, SlavesWorks.class));
            kryo.register(SlavesWorks.TreeNode.class, new FieldSerializer(kryo, SlavesWorks.TreeNode.class));
            kryo.register(SlavesWorks.class, new FieldSerializer(kryo, SlavesWorks.class));
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        SparkConf sparkConf = new SparkConf().
                setAppName("Generalized Suffix Tree");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryo.registrator", ClassRegistrator.class.getName());
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final String inputURL = args[0];
        final String outputURL = args[1];
        final int Fm = Integer.parseInt(args[2]);
        final int ELASTIC_RANGE = Integer.parseInt(args[3]);

        //开始读取文本文件
        List<String> pathList = listFiles(inputURL);
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();//终结符:文件名
        final List<String> S = new ArrayList<String>();
        final SlavesWorks masterWork = new SlavesWorks(ELASTIC_RANGE);
        for (String filename : pathList) {
            String content = readFile(filename);
            Character terminator = masterWork.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename.substring(filename.lastIndexOf('/') + 1));
        }

        Set<Character> alphabet = masterWork.getAlphabet(S);
        System.out.println("==================Start Vertical Partition=======================");
        Set<Set<String>> setOfVirtualTrees = masterWork.verticalPartitioning(S, alphabet, Fm);
        System.out.println("==================Vertical Partition Finished setOfVirtualTrees:" + setOfVirtualTrees.size() + "================");
        //分配任务
        final Broadcast<List<String>> broadcastStringList = sc.broadcast(S);
        JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<Set<String>>(setOfVirtualTrees));
//        long start = System.currentTimeMillis();
//        vtRDD.foreach(new VoidFunction<Set<String>>() {
//            public void call(Set<String> strings) throws Exception {
//                List<String> piList = new ArrayList<String>(strings);
//                List<SlavesWorks.L_B> list = new ArrayList<SlavesWorks.L_B>();
//                for (String pi : piList)
//                    list.add(masterWork.subTreePrepare(broadcastStringList.getValue(), pi));
//
//            }
//        });
//        System.out.println("end:" + (System.currentTimeMillis() - start) / 1000);
//        System.exit(0);
        //subTreePrepare
        JavaPairRDD<List<String>, List<SlavesWorks.L_B>> subtreePrepared = vtRDD.mapToPair(new PairFunction<Set<String>, List<String>, List<SlavesWorks.L_B>>() {
            public Tuple2<List<String>, List<SlavesWorks.L_B>> call(Set<String> strings) throws Exception {
                List<String> piList = new ArrayList<String>(strings);
                List<SlavesWorks.L_B> list = new ArrayList<SlavesWorks.L_B>();
                for (String pi : piList)
                    list.add(masterWork.subTreePrepare(broadcastStringList.getValue(), pi));
                return new Tuple2<List<String>, List<SlavesWorks.L_B>>(piList, list);
            }
        });
        System.out.println("==================SubTree Prepare Finished=======================");
        JavaRDD<String> resultsRDD = subtreePrepared.map(new Function<Tuple2<List<String>, List<SlavesWorks.L_B>>, String>() {
            public String call(Tuple2<List<String>, List<SlavesWorks.L_B>> listListTuple2) throws Exception {
                List<String> piList = listListTuple2._1;
                List<SlavesWorks.L_B> L_B_List = listListTuple2._2;
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 0; i < piList.size(); i++) {
                    String pi = piList.get(i);
                    SlavesWorks.L_B lb = L_B_List.get(i);
                    SlavesWorks.TreeNode root = masterWork.buildSubTree(broadcastStringList.getValue(), lb);
                    masterWork.splitSubTree(broadcastStringList.getValue(), pi, root);
                    stringBuilder.append(masterWork.traverseTree(S,root, terminatorFilename));
                }
                return stringBuilder.deleteCharAt(stringBuilder.length() - 1).toString();
            }
        });
        resultsRDD.saveAsTextFile(outputURL);
        System.out.println("=====================Tasks Done============");
    }
}