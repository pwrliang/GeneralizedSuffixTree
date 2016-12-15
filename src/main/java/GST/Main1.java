package GST;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

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
public class Main1 {
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

    private static boolean mkdir(String url) throws IOException {
        Path path = new Path(url);
        URI uri = path.toUri();
        String hdfsPath = String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsPath);

        FileSystem fileSystem = FileSystem.get(conf);
        if (!fileSystem.exists(path)) {
            fileSystem.mkdirs(path);
            return true;
        }
        return false;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        SparkConf sparkConf = new SparkConf().
                setAppName("Generalized Suffix Tree");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final String inputURL = args[0];
        final String outputURL = args[1];
        final int Fm = Integer.parseInt(args[2]);
        final int ELASTIC_RANGE = Integer.parseInt(args[3]);

        //开始读取文本文件
        List<String> pathList = listFiles(inputURL);
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();//终结符:文件名
        final List<String> S = new ArrayList<String>();
        SlavesWorks masterWork = new SlavesWorks();
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

        JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<Set<String>>(setOfVirtualTrees));
        //subTreePrepare
        JavaPairRDD<List<String>, List<SlavesWorks.L_B>> subtreePrepared = vtRDD.mapToPair(new PairFunction<Set<String>, List<String>, List<SlavesWorks.L_B>>() {
            public Tuple2<List<String>, List<SlavesWorks.L_B>> call(Set<String> strings) throws Exception {
                SlavesWorks slavesWorks = new SlavesWorks(S, ELASTIC_RANGE);
                List<String> piList = new ArrayList<String>(strings);
                List<SlavesWorks.L_B> list = new ArrayList<SlavesWorks.L_B>();
                for (String pi : piList)
                    list.add(slavesWorks.subTreePrepare(S, pi));
                return new Tuple2<List<String>, List<SlavesWorks.L_B>>(piList, list);
            }
        });
        System.out.println("==================SubTree Prepare Finished=======================");
        JavaRDD<String> rootsRDD = subtreePrepared.map(new Function<Tuple2<List<String>, List<SlavesWorks.L_B>>, String>() {
            public String call(Tuple2<List<String>, List<SlavesWorks.L_B>> listListTuple2) throws Exception {
                List<String> piList = listListTuple2._1;
                List<SlavesWorks.L_B> L_B_List = listListTuple2._2;
                SlavesWorks slavesWorks = new SlavesWorks(S);
                StringBuilder stringBuilder = new StringBuilder();
                for (int i = 0; i < piList.size(); i++) {
                    String pi = piList.get(i);
                    SlavesWorks.L_B lb = L_B_List.get(i);
                    SlavesWorks.TreeNode root = slavesWorks.buildSubTree(lb);
                    slavesWorks.splitSubTree(S, pi, root);
                    stringBuilder.append(slavesWorks.traverseTree(root, terminatorFilename));
                }
                return stringBuilder.toString();
            }
        });
        rootsRDD.saveAsTextFile(outputURL);
        System.out.println("=====================Tasks Done============");
    }
}
