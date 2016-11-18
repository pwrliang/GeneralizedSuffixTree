package GST;

import scala.util.parsing.combinator.testing.Str;

import java.io.*;
import java.util.*;

/**
 * Created by gengl on 16-11-18.
 */
public class SingleVersion {
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

    public static void main(String[] args) {
        File file = new File("/home/gengl/Documents/exset/ex3");
        SlavesWorks slavesWorks = new SlavesWorks();
        final List<String> S = new ArrayList<String>();
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();//终结符:文件名
        for (String filename : file.list()) {
            String path = file.getPath() + "/" + filename;
            String content = readLocalFile(new File(path));
            Character terminator = slavesWorks.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename);
        }
        Set<Character> alphabet = slavesWorks.getAlphabet(S);
        Set<Set<String>> setOfVirtualTrees = slavesWorks.verticalPartitioning(S, alphabet, 1 * 1024 * 1024 * 1024 / (2 * 20));
        for (Set<String> eachMachine : setOfVirtualTrees) {
            SlavesWorks works = new SlavesWorks(S, eachMachine, terminatorFilename);
            String result = works.work();
            System.out.println(result);
        }
    }
}
