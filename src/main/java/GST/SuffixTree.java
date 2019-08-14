package GST;

import GST.ERA.TreeNode;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.util.*;

public class SuffixTree {
    public enum Mode {
        SERIAL,
        PARALLEL,
        DISTRIBUTED
    }

    private Mode mode;
    private int Fm;
    private ERA era;
    List<String> S; // string list, each one is the content of input file
    private Map<Character, String> terminatorFilename; // <Terminator, Filename>
    private Map<String, String> dataSet; // <File path, content of the file>
    private Map<String, TreeNode> prefixRoot; // Note: the key has no SPLITTER/SPLITTER_INSERTION
    private JavaSparkContext sc;
    private JavaRDD<String> SRDD;


    public SuffixTree(Mode mode, int Fm) {
        this.mode = mode;
        this.Fm = Fm;
        era = new ERA();
        terminatorFilename = new HashMap<>();
        dataSet = new HashMap<>();
        S = new ArrayList<>();
    }

    public void build(String folderPath) {
        switch (mode) {
            case SERIAL:
                break;
            case PARALLEL:
                break;
            case DISTRIBUTED:
                SparkConf conf = new SparkConf().
                        setAppName("GST");
                conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                conf.set("spark.kryo.registrator", Main.ClassRegistrator.class.getName());
                conf.set("spark.kryoserializer.buffer.max", "2047");

                sc = new JavaSparkContext(conf);
                JavaPairRDD<String, String> inputData = sc.wholeTextFiles(folderPath);
                dataSet = inputData.collectAsMap();
                break;
            default:
        }

        for (String path : dataSet.keySet()) {
            String filename = new Path(path).getName();
            Character terminator = era.nextTerminator();
            S.add(dataSet.get(path) + terminator); // append terminator to the end of text
            terminatorFilename.put(terminator, filename);
        }

        Set<Character> alphabet = era.getAlphabet(S);//扫描串获得字母表


        if (mode == Mode.DISTRIBUTED) {
            // Algorithm: VerticalPartitioning
            SRDD = sc.parallelize(S, S.size()).cache();
            Set<Set<String>> setOfVirtualTrees = era.verticalPartitioningSpark(SRDD, alphabet, Fm);//开始垂直分区
            SRDD.unpersist();

            Broadcast<List<String>> broadcastStringList = sc.broadcast(S);
            JavaRDD<Set<String>> vtRDD = sc.parallelize(new ArrayList<>(setOfVirtualTrees), setOfVirtualTrees.size());

            JavaRDD<Map<String, TreeNode>> tmp = vtRDD.map((Function<Set<String>, Map<String, TreeNode>>) prefixSet -> {
                List<String> mainString = broadcastStringList.value();
                Map<String, List<int[]>> prefixLoc = era.getPrefixLoc(mainString, prefixSet);//一次寻找前缀集合中所有前缀的位置
                Map<String, TreeNode> prefixRoot = new HashMap<>();

                for (String prefix : prefixLoc.keySet()) {
                    // Algorithm: SubTreePrepare
                    ERA.L_B lb = era.subTreePrepare(mainString, prefix, prefixLoc.get(prefix));

                    // Algorithm: BuildSubTree
                    TreeNode root = era.buildSubTree(mainString, lb);
                    era.splitSubTree(mainString, prefix, root);

                    // put prefix without splitter and root
                    prefixRoot.put(ERA.noSplitter(prefix), root);
                }
                return prefixRoot;
            });
        }
    }

}
