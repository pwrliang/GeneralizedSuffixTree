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

    static List<String> sort(List<String> input) {
        Collections.sort(input, new Comparator<String>() {
            public int compare(String o1, String o2) {
                String filename1 = o1.split(":")[0].split(" ")[1];
                Integer index1 = Integer.parseInt(o1.split(":")[1]);
                String filename2 = o2.split(":")[0].split(" ")[1];
                Integer index2 = Integer.parseInt(o2.split(":")[1]);
                if (filename1.compareTo(filename2) == 0)
                    if (index1 == index2)
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
    public static void main(String[] args) throws IOException {
//        clearFolder();
//        System.setOut(new PrintStream("D:\\Liang_Projects\\exset\\新建文本文档.txt"));
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
        }

        Set<Character> alphabet = masterWorks.getAlphabet(S);
        for (int i = 1; i < 10000; i++) {
            int range = 0;
            int Fm = 0;
            while (range == 0) {
                range = new Random().nextInt(Integer.MAX_VALUE);
            }
            while (Fm == 0) {
                Fm = new Random().nextInt(Integer.MAX_VALUE);
            }
            Set<Set<String>> setOfVirtualTrees = masterWorks.verticalPartitioning(S, alphabet, Fm);
            System.setOut(new PrintStream("D:\\Liang_Projects\\exset\\output\\" + Fm + " " + range + ".txt"));
            List<String> result = new ArrayList<String>();
            for (Set<String> virtualTrees : setOfVirtualTrees) {
                SlavesWorks slavesWorks = new SlavesWorks(S, virtualTrees, terminatorFilename, "", range);
                String[] subTree = slavesWorks.workEx().split("\n");
                for (String tree : subTree)
                    result.add(tree);
            }
            if (i % 1000 == 0)
                System.out.println(i);
            sort(result);
            for (String leaf : result)
                System.out.println(leaf);
        }
    }
}
