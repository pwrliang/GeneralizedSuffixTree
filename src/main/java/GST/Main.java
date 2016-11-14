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
    public static String readLocalFile(File file) {
        StringBuilder sb = new StringBuilder();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null)
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
        return sb.toString();
    }

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

    public static String readFile(FileSystem fileSystem, Path path) throws IOException {
        FSDataInputStream inputStream = fileSystem.open(path);
        StringBuilder sb = new StringBuilder();
        String line;
        while ((line = inputStream.readLine()) != null) {
            sb.append(line);
        }
        inputStream.close();
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

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setAppName("Generalized Suffix Tree");
        final JavaSparkContext sc = new JavaSparkContext(conf);
        String inputURL = args[0];
//        String outputURL = args[1];
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
        SlavesWorks slavesWorks = new SlavesWorks();
        final List<String> S = new ArrayList<String>();
        for (String filename : pathList) {
            String content = readFile(fileSystem, new Path(filename));
            Character terminator = slavesWorks.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename.substring(filename.lastIndexOf('/')));

        }
        Set<Character> alphabet = slavesWorks.getAlphabet(S);
        Set<Set<String>> setOfVirtualTrees = slavesWorks.verticalPartitioning(S, alphabet, 2 * 1024 * 1024 / 10);
        System.out.println("Vertical Partition Finished");
        //分配任务
        JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<Set<String>>(setOfVirtualTrees));
        JavaRDD<SlavesWorks> works = vtRDD.map(new Function<Set<String>, SlavesWorks>() {
            public SlavesWorks call(Set<String> v1) throws Exception {
                return new SlavesWorks(S, v1, terminatorFilename);
            }
        });
        //执行任务
        works.foreach(new VoidFunction<SlavesWorks>() {
            public void call(SlavesWorks slavesWorks) throws Exception {
                List<String> result = slavesWorks.work();
//                JavaRDD<String> rdd = sc.parallelize(result);
//                rdd.foreach(new VoidFunction<String>() {
//                    public void call(String s) throws Exception {
//                        outputStream.writeChars(s);
//                    }
//                });
            }
        });
        System.out.println("end===========================");
    }
}
