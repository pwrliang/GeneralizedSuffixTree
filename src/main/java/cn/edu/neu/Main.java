package cn.edu.neu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Created by Liang on 16-11-9.
 */
public class Main {
    public static String readFile(File file) {
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

    public static void WriteToFile(String filename, String line) {
        File file = new File(filename);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(file));
            bw.write(line);
            bw.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bw != null)
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }
    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Generalized Suffix Tree");
        JavaSparkContext sc = new JavaSparkContext(conf);

        File folder = new File(args[0]);
//        File folder = new File("/home/lib/Documents/exset/ex3");
        String[] fileNames = folder.list();
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();
        SlavesWorks slavesWorks = new SlavesWorks();
        final List<String> S = new ArrayList<String>();
        for (String filename : fileNames) {
            File txtFile = new File(folder.getPath() + "/" + filename);
            String content = readFile(txtFile);
            Character terminator = slavesWorks.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename);
        }
        Set<Character> alphabet = slavesWorks.getAlphabet(S);
        Set<Set<String>> setOfVirtualTrees = slavesWorks.verticalPartitioning(S, alphabet, 2 * 1024 * 1024 / 10);
        System.out.println("Vertical Partition Finished");
        JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<Set<String>>(setOfVirtualTrees));
        JavaRDD<SlavesWorks> works = vtRDD.map(new Function<Set<String>, SlavesWorks>() {
            public SlavesWorks call(Set<String> v1) throws Exception {
                return new SlavesWorks(S, v1, terminatorFilename);
            }
        });
        works.foreach(new VoidFunction<SlavesWorks>() {
            public void call(SlavesWorks slavesWorks) throws Exception {
                slavesWorks.work();
            }
        });
        System.out.println("end");
    }
}
