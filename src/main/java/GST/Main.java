package GST;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.Options;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.AccumulatorV2;
import scala.util.parsing.combinator.testing.Str;

/**
 * Created by Liang on 16-11-9.
 */
public class Main {

    public static String readFile(String url) throws IOException {
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

    public static void appendToFile(String url, String line) throws IOException {
        Path path = new Path(url);
        URI uri = path.toUri();
        String hdfsPath = String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsPath);//hdfs://master:9000
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataOutputStream outputStream = fileSystem.append(path);
        outputStream.writeChars(line);
        outputStream.close();
    }

    static void writeToFile(String outputURL, String filename, String content) throws IOException {
        Path path = new Path(outputURL + "/" + filename);
        URI uri = path.toUri();
        String hdfsPath = String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsPath);//hdfs://master:9000
        FileSystem fileSystem = FileSystem.get(conf);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fileSystem.create(path)));
        bufferedWriter.write(content);
        bufferedWriter.close();
    }

    static void writeToLocal(String path, String content) throws IOException {
        File file = new File(path);
        if (file.exists())
            file.delete();
        file.createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
//        writer.write(content);
        writer.append(content);
        writer.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        SparkConf sparkConf = new SparkConf().
                setAppName("Generalized Suffix Tree");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);
        final String inputURL = args[0];
        final String outputURL = args[1];
        final int Fm = Integer.parseInt(args[2]);
        final int ELASTIC_RANGE = Integer.parseInt(args[3]);

        final Date startDate = new Date();
        //开始读取文本文件
        List<String> pathList = listFiles(inputURL);
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();//终结符:文件名
        SlavesWorks masterWork = new SlavesWorks();
        final List<String> S = new ArrayList<String>();
        for (String filename : pathList) {
            String content = readFile(filename);
            Character terminator = masterWork.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename.substring(filename.lastIndexOf('/') + 1));
        }
        Set<Character> alphabet = masterWork.getAlphabet(S);
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
        //50000 1000 data set
        //50000 1000  35min
        System.out.println("==================Start Vertical Partition=======================");
        Set<Set<String>> setOfVirtualTrees = masterWork.verticalPartitioning(S, alphabet, Fm);
        System.out.println("==================Vertical Partition Finished setOfVirtualTrees:" + setOfVirtualTrees.size() + "================");
        //分配任务
        JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<Set<String>>(setOfVirtualTrees));
        JavaRDD<SlavesWorks> works = vtRDD.map(new Function<Set<String>, SlavesWorks>() {
            public SlavesWorks call(Set<String> v1) throws Exception {
                return new SlavesWorks(S, v1, terminatorFilename, outputURL, ELASTIC_RANGE);
            }
        });
//      执行任务
        System.out.println("=====================Start Tasks============");
        works.foreach(new VoidFunction<SlavesWorks>() {
            public void call(SlavesWorks slavesWorks) throws Exception {
                slavesWorks.work();
            }
        });
        System.out.println("=====================Tasks Done============");
        writeToFile(outputURL, "SUCCESS", String.format("START:%s\nEND:%s\n", startDate, new Date().toString()));
        System.out.println("==============end===============");
    }
}
