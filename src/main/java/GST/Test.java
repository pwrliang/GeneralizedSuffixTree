package GST;

import java.io.*;
import java.util.*;

/**
 * Created by Liang on 2017/4/1.
 */
public class Test {
    static int FmSelector(int fileSize) {
        if (fileSize < 5000000) //5000 1000
            return 30000;
        else if (fileSize < 30000000)//50000 1000
            return 30000;
        else if (fileSize < 50000000)//500000 20
            return 60000;
        else if (fileSize < 80000000)//50000 5000
            return 40000;
        else //500000 1000
            return 60000;
    }

    static void read(String path, Map<String, String> map) throws IOException {
        File files = new File(path);
        for (File file : files.listFiles()) {
            BufferedReader reader = new BufferedReader(new FileReader(file));
            map.put(file.getName(), reader.readLine());
            reader.close();
        }
    }

    public static void main(String[] args) throws IOException {
        final ERA era = new ERA();
        final String inputURL = "C:\\Users\\Liang\\Documents\\input\\5000 1000";
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();//终结符:文件名
        final List<String> S = new ArrayList<String>();

        //开始读取文本文件
        //key filename value content
        Map<String, String> dataSet = new HashMap<String, String>();
        read(inputURL, dataSet);

        for (String filename : dataSet.keySet()) {
            Character terminator = era.nextTerminator();
            S.add(dataSet.get(filename) + terminator);//append terminator to the end of text
            terminatorFilename.put(terminator, filename);
        }
//
        int lengthForAll = 0;
        for (String s : S)
            lengthForAll += s.length();
        int Fm = FmSelector(lengthForAll);
        Set<Character> alphabet = ERA.getAlphabet(S);//扫描串获得字母表
//        {
//            long start = System.currentTimeMillis();
//            Set<Set<String>> setOfVirtualTrees = era.verticalPartitioning(S, alphabet, Fm);//开始垂直分区
//            System.out.println("normal:" + (System.currentTimeMillis() - start));
//        }
        {
            Set<Set<String>> setOfVirtualTrees = era.verticalPartitioningAlpha(S, alphabet, Fm, 100);//开始垂直分区
            long start = System.currentTimeMillis();
            for (Set<String> piSet : setOfVirtualTrees) {
                for (String pi : piSet) {
                    ERA.L_B lb = era.subTreePrepare(S, pi);
                    ERA.TreeNode root = era.buildSubTree(S, lb);
                    era.splitSubTree(S, pi, root);
//                    System.out.println(pi);
                }
            }
            System.out.println(System.currentTimeMillis() - start);
            System.out.println("line1-7 " + era.line1_7);
            System.out.println("line9-12 " + era.line9_12);
            System.out.println("line13-15 " + era.line13_15);
            System.out.println("sortR " + era.sortR);
            System.out.println("maintainI " + era.maintainI);
            System.out.println("line16-24 " + era.line16_24);
            System.out.println("copy " + era.lineCopy);
        }
//        List<String> strings = new ArrayList<String>();
//        strings.add("TGGTGGTGGTGCGGTGATGGTGC"+era.nextTerminator());
//        era.subTreePrepare(strings,"TG");
//        era.subTreePrepareAlpha(strings,"TG");
    }
}
