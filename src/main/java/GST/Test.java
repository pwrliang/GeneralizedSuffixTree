package GST;

import java.io.File;
import java.util.*;

/**
 * Created by Lib on 2016/12/25.
 */
public class Test {
    public static void main(String[] args) {
        File folder = new File("D:\\Liang_Projects\\exset\\ex1");
        String[] fileNames = folder.list();
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();
        final ERA masterWorks = new ERA();
        final List<String> S = new ArrayList<String>();
        for (String filename : fileNames) {
            File txtFile = new File(folder.getPath() + "/" + filename);
            String content = SingleVersion.readLocalFile(txtFile);
            Character terminator = masterWorks.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename);
        }

        int range=464;
        int Fm=6;

        final Set<Character> alphabet = ERA.getAlphabet(S);
        final List<String> rightResult = SingleVersion.readLocalFileLine(new File("D:\\Liang_Projects\\exset\\res1.txt"));
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
        SingleVersion.sort(result);
        for(String line:result)
            System.out.println(line);
        if (rightResult.size() != result.size())
            System.out.println(String.format("Fm:%d range:%d %d %d", Fm, range,rightResult.size(),result.size()));
        for (int n = 0; n < rightResult.size(); n++)
            if (!rightResult.get(n).equals(result.get(n))) {
                System.out.println(String.format("Fm:%d range:%d", Fm, range));
                System.out.println(rightResult.get(n) + " " + result.get(n));
                System.exit(1);
            }
    }
}
