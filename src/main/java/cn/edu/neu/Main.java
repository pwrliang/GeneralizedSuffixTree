package cn.edu.neu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;

import cn.edu.neu.ERA.*;

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

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Generalized Suffix Tree");
        JavaSparkContext sc = new JavaSparkContext(conf);

        File folder = new File("/home/lib/Documents/exset/ex1");
        String[] fileNames = folder.list();
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();
        ERA era = new ERA();
        final List<String> S = new ArrayList<String>();
        for (String filename : fileNames) {
            File txtFile = new File(folder.getPath() + "/" + filename);
            String content = readFile(txtFile);
            Character terminator = era.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename);
        }
        Set<Character> alphabet = era.getAlphabet(S);
        Set<Set<String>> setOfVirtualTrees = era.verticalPartitioning(S, alphabet, 2 * 1024 * 1024 / 10);
        System.out.println("Vertical Partition Finished");
        for (Set<String> virtualTrees : setOfVirtualTrees) {
            for (final String s : virtualTrees) {
                ERA instance = new ERA();
                Object[] L_B = instance.subTreePrepare(S, s);
                TreeNode root = instance.buildSubTree(S, (List<int[]>) L_B[0], (List<TypeB>) L_B[1]);
                instance.splitSubTree(S, s, root);
                instance.traverseTree(root, terminatorFilename);
            }
        }
    }
}
