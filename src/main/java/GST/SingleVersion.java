package GST;

import java.io.*;
import java.util.*;
/**
 * Created by gengl on 16-11-18.
 * This is the single version
 */
public class SingleVersion {
    private static String readLocalFile(File file) {
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

    public static void main(String[] args) throws IOException {
//        clearFolder();
        File folder = new File("D:\\Liang_Projects\\exset\\ex3");
        String[] fileNames = folder.list();
        final Map<Character, String> terminatorFilename = new HashMap<Character, String>();
        SlavesWorks masterWorks = new SlavesWorks();
        final List<String> S = new ArrayList<String>();
        for (String filename : fileNames) {
            File txtFile = new File(folder.getPath() + "/" + filename);
            String content = readLocalFile(txtFile);
            Character terminator = masterWorks.nextTerminator();
            S.add(content + terminator);
            terminatorFilename.put(terminator, filename);
//            System.out.println(filename);
        }


        Set<Character> alphabet = masterWorks.getAlphabet(S);
        Set<Set<String>> setOfVirtualTrees = masterWorks.verticalPartitioning(S, alphabet,  Integer.MAX_VALUE);
//        System.out.println("Vertical Partition Finished");
//        System.out.println(setOfVirtualTrees.size());
        for (Set<String> virtualTrees : setOfVirtualTrees) {
            SlavesWorks slavesWorks = new SlavesWorks(S, virtualTrees, terminatorFilename,"",1000);
            System.out.print(slavesWorks.workEx());
        }
    }
}
