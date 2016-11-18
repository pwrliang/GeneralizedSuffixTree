package GST;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import scala.util.parsing.combinator.testing.Str;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

    public static void main(String[] args) throws IOException {
        File folder = new File("/home/gengl/Documents/exset/ex3");
//        String[] fileNames = folder.list();
//        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();
        SlavesWorks masterWorks = new SlavesWorks();
//        final List<String> S = new ArrayList<String>();
//        for (String filename : fileNames) {
//            File txtFile = new File(folder.getPath() + "/" + filename);
//            String content = readLocalFile(txtFile);
//            Character terminator = masterWorks.nextTerminator();
//            S.add(content + terminator);
//            terminatorFilename.put(terminator, filename);
//            System.out.println(filename);
//        }


        List<String> pathList = Main.listFiles("hdfs://master:9000/exset/ex3");
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();//终结符:文件名
        SlavesWorks masterWork = new SlavesWorks();
        final List<String> S = new ArrayList<String>();
        for (String filename : pathList) {
            System.out.println(filename);
            String content = Main.readFile(filename);
            Character terminator = masterWork.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename.substring(filename.lastIndexOf('/') + 1));
        }

        Set<Character> alphabet = masterWorks.getAlphabet(S);
        long lastTime = System.currentTimeMillis();
        Set<Set<String>> setOfVirtualTrees = masterWorks.verticalPartitioning(S, alphabet, 1 * 1024 * 1024 * 1024 / (2 * 20));
        System.out.println("Vertical Partition Finished");
        for (final Set<String> virtualTrees : setOfVirtualTrees) {
//            SlavesWorks slavesWorks = new SlavesWorks(S, virtualTrees, terminatorFilename);
//            System.out.println(slavesWorks.work());
        }
    }
}
