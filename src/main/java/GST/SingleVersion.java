package GST;

import java.io.*;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by gengl on 16-11-18.
 * This is the single version
 */
public class SingleVersion {
    static String readLocalFile(File file) {
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

    static List<String> readLocalFileLine(File file) {
        List<String> lines = new ArrayList<String>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
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
        return lines;
    }

    private static void writeToLocal(String path, String content) throws IOException {
        File file = new File(path);
        if (file.exists())
            file.delete();
        file.createNewFile();
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
//        writer.write(content);
        writer.append(content);
        writer.close();
    }

    public static void clearFolder() {
        File folder = new File("D:\\Liang_Projects\\exset\\graph");
        for (String fileName : folder.list()) {
            File file = new File(folder + "/" + fileName);
            file.delete();
        }
    }

    static List<String> sort(List<String> input) {
        Collections.sort(input, new Comparator<String>() {
            public int compare(String o1, String o2) {
                String filename1 = o1.split(":")[0].split(" ")[1];
                Integer index1 = Integer.parseInt(o1.split(":")[1]);
                String filename2 = o2.split(":")[0].split(" ")[1];
                Integer index2 = Integer.parseInt(o2.split(":")[1]);
                if (filename1.compareTo(filename2) == 0)
                    if (index1.equals(index2))
                        return 0;
                    else if (index1 > index2)
                        return 1;
                    else
                        return -1;
                return filename1.compareTo(filename2);
            }
        });
        return input;
    }

    //split1 695ms split2 626ms
    public static void main(String[] args) throws IOException, InterruptedException {
        File folder = new File("D:\\Liang_Projects\\exset\\ex2");
        String[] fileNames = folder.list();
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();
        final ERA masterWorks = new ERA();
        final List<String> S = new ArrayList<String>();
        for (String filename : fileNames) {
            File txtFile = new File(folder.getPath() + "/" + filename);
            String content = readLocalFile(txtFile);
            Character terminator = masterWorks.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename);
        }

        final Set<Character> alphabet = ERA.getAlphabet(S);
        final List<String> rightResult = readLocalFileLine(new File("D:\\Liang_Projects\\exset\\res2.txt"));
        final ThreadPoolExecutor executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(50);
        for (int i = 1; i < 100000; i++) {
            final int finalI = i;
            executorService.execute(new Runnable() {
                public void run() {
                    int range = 0;
                    int Fm = 0;
                    while (range == 0) {
                        range = new Random().nextInt(1000);
                    }
                    while (Fm == 0) {
                        Fm = new Random().nextInt(100);
                    }
                    Set<Set<String>> setOfVirtualTrees = masterWorks.verticalPartitioning(S, alphabet, Fm);
                    List<String> result = new ArrayList<String>();
                    ERA slavesWorks = new ERA(range);
                    for (Set<String> virtualTrees : setOfVirtualTrees) {
                        for (String p : virtualTrees) {
                            ERA.L_B lb = slavesWorks.subTreePrepare(S, p);
                            ERA.TreeNode treeNode = slavesWorks.buildSubTree(S, lb);
                            slavesWorks.splitSubTree(S, p, treeNode);
                            String subTree = slavesWorks.traverseTree(S,treeNode, terminatorFilename);
                            String[] lines = subTree.split("\n");
                            for (String line : lines)
                                result.add(line);
                        }
                    }
                    sort(result);

                    if (rightResult.size() != result.size())
                        System.out.println(String.format("Fm:%d range:%d %d %d", Fm, range,rightResult.size(),result.size()));
                    for (int n = 0; n < rightResult.size(); n++)
                        if (!rightResult.get(n).equals(result.get(n))) {
                            System.out.println(String.format("Fm:%d range:%d", Fm, range));
                            System.out.println(rightResult.get(n) + " " + result.get(n));
                            System.exit(0);
                        }
                    if (finalI % 1000 == 0)
                        System.out.println(finalI);
                }
            });
        }
        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
    }
}