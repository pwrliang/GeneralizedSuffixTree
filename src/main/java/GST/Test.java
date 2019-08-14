package GST;

import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

public class Test {
    public static void main(String[] args) throws IOException {

        if (args.length != 1)
            throw new IllegalArgumentException();

        File folder = new File(args[0]);

        if (!folder.isDirectory())
            return;

        Map<String, String> dataSet = new HashMap<>();
        Queue<File> queue = new LinkedList<>();
        queue.offer(folder);


        while (!queue.isEmpty()) {
            int size = queue.size();
            while (size-- > 0) {
                folder = queue.poll();

                for (File file : folder.listFiles()) {
                    if (file.isDirectory())
                        queue.offer(file);
                    else {
                        BufferedReader reader = new BufferedReader(new FileReader(file));
                        String line;
                        StringBuilder result = new StringBuilder();

                        while ((line = reader.readLine()) != null) {
                            result.append(line);
                        }

                        dataSet.put(file.getPath(), result.toString());
                    }
                }
            }
        }


        final ERA era = new ERA();
        List<String> S = new ArrayList<>();
        Map<Character, String> terminatorFilename = new HashMap<>(); // <Terminator, Path>

        Character terminator1 = 0;
        for (String path : dataSet.keySet()) {
            String filename = new Path(path).getName();
            Character terminator = era.nextTerminator();
            terminator1 = terminator;
            S.add(dataSet.get(path) + terminator);//append terminator to the end of text
            terminatorFilename.put(terminator, filename);
        }

        Set<Character> alphabet = era.getAlphabet(S); // Scan all strings to get alphabet
        int Fm = 10000;

        System.out.println("verticalPartitioning1");
        Set<Set<String>> setOfVirtualTrees = era.verticalPartitioning1(S, alphabet, Fm); // Starting vertical partition

        System.out.println("verticalPartitioning1 done");
        Map<String, ERA.TreeNode> prefixRoot = new HashMap<>();

        String target = "GTGC" + terminator1;

        Map<String, ERA.TreeNode> finalRoot = new HashMap<>();

        for (Set<String> prefixSet : setOfVirtualTrees) {
            Map<String, List<int[]>> prefixLoc = era.getPrefixLoc(S, prefixSet); // 一次寻找前缀集合中所有前缀的位置
            for (String prefix : prefixLoc.keySet()) {
                Set<String> result = new HashSet<>();

                ERA.L_B lb = era.subTreePrepare(S, prefix, prefixLoc.get(prefix));
                ERA.TreeNode root = era.buildSubTree(S, lb);
                era.splitSubTree(S, prefix, root);
                prefixRoot.put(prefix, root);

                String prefixNoSplitter = ERA.noSplitter(prefix);

                if(finalRoot.containsKey(prefixNoSplitter))
                    System.err.println("Error: duplicated prefixNoSplitter!!!");
                finalRoot.put(prefixNoSplitter, root);

//                System.out.println(prefix);
//                era.traverseTree(S, root, terminatorFilename, result);
//
//                for (String s : result) {
//                    System.out.println(s);
//                }


//                if (target.startsWith(prefixNoSplitter)) {
//                    System.out.println(endsWith(target, S, root.leftChild));
//                }
            }
        }



//        String target1 = "GTGAC";
//        boolean contains = false;
//
//        for (Map.Entry<String, ERA.TreeNode> entry : finalRoot.entrySet()) {
//            String prefix = entry.getKey();
//            ERA.TreeNode root = entry.getValue();
//
//            if (target1.length() <= prefix.length() && prefix.startsWith(target1)) {
//                if (prefix.startsWith(target1)) {
//                    contains = true;
//                    break;
//                }
//            } else if (target1.startsWith(prefix)) {
//                contains = contains(target1, 0, S, root.leftChild);
//                if (contains)
//                    break;
//            }
//        }
//
//        System.out.println(contains);

    }

    boolean endsWith(String target, List<String> S, ERA.TreeNode root) {
        boolean exists = false;
        if (root == null)
            return false;
        String textOnNode = S.get(root.index).substring(root.start, root.end);

        // if root is leaf node
        if (root.suffix_index != -1) {
            exists = textOnNode.equals(target); // the text on the leaf node equals with target

            if (exists)
                return true;
            else {
                // if doesn't equal, compare with sibling
                while ((root = root.rightSibling) != null) {
                    if (endsWith(target, S, root)) {
                        exists = true;
                        break;
                    }
                }
            }
        } else { // if root is internal node
            if (target.startsWith(textOnNode))
                exists = endsWith(target.substring(textOnNode.length()), S, root.leftChild);
        }

        return exists;
    }

    /**
     * @param target The target string to compare with.
     * @param S      The String list
     * @param root   The tree root
     * @return return true if the target can be found, else return false.
     */
    static public boolean contains(String target, int idx, List<String> S, ERA.TreeNode root) {
        boolean exists = false;
        if (root == null)
            return false;
        String text = S.get(root.index);
        String textOnNode = text.substring(root.start, root.end);

        if (idx == target.length())
            return true;

        int i = idx;
        int j = root.start;


        boolean gotoSib = false;

        while (i < target.length() && j < root.end) {
            if (target.charAt(i) != text.charAt(j)) {
                gotoSib = true;
                i = 0; // reset index and compare with sibling
                break;
            }
            i++;
            j++;
        }

        if (gotoSib) {
            while ((root = root.rightSibling) != null) {
                if (contains(target, i, S, root)) {
                    exists = true;
                    break;
                }
            }
        } else {
            if (i == target.length())
                exists = true;
            else {
                if (j == root.end) {
                    if (root.leftChild != null)
                        exists = contains(target, i, S, root.leftChild);
                }
            }
        }

        return exists;
    }
}
