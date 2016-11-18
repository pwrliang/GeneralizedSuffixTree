package GST;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.net.URI;
import java.util.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

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
        FSDataInputStream inputStream = fileSystem.open(path);
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = inputStream.readLine()) != null) {
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

    public static void writeToFile(String url, String line) throws IOException {
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

    static void writeTest(String outputURL,String filename,String content) throws IOException {
        Path path = new Path(outputURL + "/" + filename );
        URI uri = path.toUri();
        String hdfsPath = String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort());
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsPath);//hdfs://master:9000
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataOutputStream outputStream = fileSystem.create(path);
        outputStream.writeChars(content);
        outputStream.close();
    }

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Generalized Suffix Tree");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        final String inputURL = args[0];
        final String outputURL = args[1];
        System.out.println(inputURL+"---------------"+outputURL);
        Path inputPath = new Path(inputURL);
        //初始化HDFS
        URI uri = inputPath.toUri();
        String hdfsPath = String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort());
        Configuration hdfsConf = new Configuration();
        hdfsConf.set("fs.defaultFS", hdfsPath);//hdfs://master:9000
        FileSystem fileSystem = FileSystem.get(hdfsConf);
        //开始读取文本文件
        List<String> pathList = listFiles(inputURL);
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();//终结符:文件名
        SlavesWorks masterWork = new SlavesWorks();
        final List<String> S = new ArrayList<String>();
        for (String filename : pathList) {
            System.out.println(filename);
            String content = readFile(filename);
            Character terminator = masterWork.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename.substring(filename.lastIndexOf('/')));
            System.out.println("filename:"+filename.substring(filename.lastIndexOf('/')+1));
        }
        Set<Character> alphabet = masterWork.getAlphabet(S);
        Set<Set<String>> setOfVirtualTrees = masterWork.verticalPartitioning(S, alphabet, 1 * 1024 * 1024 * 1024 / (2 * 20));
        System.out.println("Vertical Partition Finished");
        //分配任务
        JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<Set<String>>(setOfVirtualTrees));
        JavaRDD<SlavesWorks> works = vtRDD.map(new Function<Set<String>, SlavesWorks>() {
            public SlavesWorks call(Set<String> v1) throws Exception {
                SlavesWorks slavessWorks= new SlavesWorks(S, v1, terminatorFilename);
                slavessWorks.setOutputPath(outputURL);
                return slavessWorks;
            }
        });
        final List<String> results = new ArrayList<String>();
        //执行任务
        works.foreach(new VoidFunction<SlavesWorks>() {
            public void call(SlavesWorks slavesWorks) throws Exception {
                String result = slavesWorks.work();
                Path path = new Path(slavesWorks.getOutputPath()+"/part-"+slavesWorks.hashCode());
                URI uri = path.toUri();
                String hdfsPath = String.format("%s://%s:%d", uri.getScheme(), uri.getHost(), uri.getPort());
                Configuration conf = new Configuration();
                conf.set("fs.defaultFS", hdfsPath);//hdfs://master:9000
                FileSystem fileSystem = FileSystem.get(conf);
                FSDataOutputStream outputStream = fileSystem.create(path);
                outputStream.writeChars(result);
                outputStream.close();
            }
        });
        System.out.println(results.size());
        System.out.println("end===========================");
    }
}
