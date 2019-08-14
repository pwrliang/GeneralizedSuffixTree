package GST;

import com.hankcs.algorithm.AhoCorasickDoubleArrayTrie;
import org.ahocorasick.trie.Emit;
import org.ahocorasick.trie.Trie;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.*;
import java.util.*;

/**
 * Created by lib on 16-11-11.
 * This is the implementation of Era
 */
public class ERA implements Serializable {
    static class TreeNode implements Serializable, Cloneable {
        int index;//字符串S列表的索引，代表第几个串
        int start;//串中起始位置(包括)
        int end;//串中结束为止(不包括)
        int suffix_index = -1;//叶子结点专用，主串中的起始位置
        TreeNode parent;
        TreeNode leftChild;
        TreeNode rightSibling;

        private TreeNode() {
        }

        private TreeNode(int index, int start, int end) {
            this.start = start;
            this.end = end;
            this.index = index;
        }

        @Override
        protected TreeNode clone() {
            TreeNode treeNode = null;
            try {
                treeNode = (TreeNode) super.clone();
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
            return treeNode;
        }
    }

    static class L_B implements Serializable {
        List<int[]> L;
        int[] B;

        L_B(List<int[]> L, int[] B) {
            this.L = L;
            this.B = B;
        }

        List<int[]> getL() {
            return L;
        }

        int[] getB() {
            return B;
        }
    }

    private char terminator = 43000; //起始终结符
    private static final char SPLITTER_INSERTION = 57001;//拆分并插入叶节点
    private static final char SPLITTER = 57002;//只拆分，不插入叶节点

    /**
     * 判断字符是否为终结符
     *
     * @param c 被测试的字符
     * @return 是终结符返回true否则返回false
     */
    private boolean isTerminator(char c) {
        return c >= 43000 && c <= 57000;
    }

    /**
     * 产生不重复的终结符
     */
    public char nextTerminator() {
        char term = terminator++;
        if (!isTerminator(term))
            throw new IllegalStateException("Too many terminators!");
        return term;
    }

    /**
     * 返回算法参数-弹性范围
     */
    private int getRangeOfSymbols(int L_) {
        int bufferSize = 256 * 1024 * 1024;
        if (L_ <= 0)
            L_ = 1;
        int range = bufferSize / L_;
        if (range == 0)
            return 1000;
        return range;
    }

    /**
     * 扫描所有串，生成字母表
     */
    public Set<Character> getAlphabet(List<String> S) {
        Set<Character> alphabet = new HashSet<>();
        for (String line : S) {
            for (int j = 0; j < line.length() - 1; j++) {
                char ch = line.charAt(j);
                if (isTerminator(ch) || ch == SPLITTER_INSERTION || ch == SPLITTER)
                    throw new IllegalStateException("Found internal character in the string! The the unicode value between 43000 ~ 57000, 57001 and 57002 are preserved for internal usage");
                alphabet.add(ch);
            }
        }
        return alphabet;
    }

    public static String noSplitter(String input) {
        StringBuilder sb = new StringBuilder(input.length());
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            if (ch != SPLITTER && ch != SPLITTER_INSERTION) {
                sb.append(ch);
            }
        }
        return sb.toString();
    }

    /**
     * 垂直分区
     *
     * @param S        字符串列表
     * @param alphabet 字母表
     * @param Fm       参数Fm
     * @return 返回集合列表，每个元素是集合，集合内容是pi
     */
    Set<Set<String>> verticalPartitioningSpark(JavaRDD<String> S, Set<Character> alphabet, int Fm) {
        Set<Set<String>> virtualTree = new HashSet<>();
        List<String> P_ = new ArrayList<>();
        List<String> P = new ArrayList<>();
        final Map<String, Integer> fpiList = new HashMap<>();
        //每个key一个队列

        //如果c是原生类型，就用+""转换，如果c是包装类型，就用toString
        for (Character s : alphabet)
            P_.add(s.toString());
        //////////////
        while (!P_.isEmpty()) {
            final int P_Size = P_.size();
            Map<String, Integer> currentFpiList = new HashMap<>(P_Size);
            //当pi对应的Set长度大于1，则需要拆分插入，频率为0跳过该pi，频率为1直接扩展一个字符
            final Map<String, Set<Character>> piNext = new HashMap<>(P_Size);//key pi value 下一个字符（不包括终结符）
            Map<String, Boolean> piTerminator = new HashMap<>(P_Size);//key pi value pi下一个字符是否是终结符
            final List<String> prefixList = new ArrayList<>(P_Size);
            for (String pi : P_) {//对每个pi
                String piWithoutSplitter = noSplitter(pi);
                piTerminator.put(piWithoutSplitter, false);
                prefixList.add(piWithoutSplitter);
            }
            //Map<String,Tuple2<Character,Integer>> 前缀，前缀下一个字符集合，出现频率
            //String,Tuple2<Character,Integer> //(prefix ，(下一个字符，1))
            //对于每个串，找字串集合中的元素在该串中是否存在
            JavaRDD<Set<Tuple2<String, Tuple2<Set<Character>, Integer>>>> locRDD = S.map(new Function<String, Set<Tuple2<String, Tuple2<Set<Character>, Integer>>>>() {
                @Override
                public Set<Tuple2<String, Tuple2<Set<Character>, Integer>>> call(String line) throws Exception {
                    Set<Tuple2<String, Tuple2<Set<Character>, Integer>>> result = new HashSet<>();
                    Map<String, Integer> freqMap = new HashMap<>();
                    Map<String, Set<Character>> nextCharMap = new HashMap<>();
                    Trie trie = Trie.builder().addKeywords(prefixList).build();

                    Collection<Emit> piList = trie.parseText(line);
                    for (Emit piWithoutSplitter : piList) {
                        Character nextChar = line.charAt(piWithoutSplitter.getEnd() + 1);
                        String prefix = piWithoutSplitter.getKeyword();
                        Set<Character> nextCharSet = nextCharMap.get(prefix);
                        if (nextCharSet == null) {
                            nextCharSet = new HashSet<>();
                            nextCharMap.put(prefix, nextCharSet);
                        }
                        nextCharSet.add(nextChar);
                        Integer freq = freqMap.get(prefix);
                        if (freq == null) {
                            freqMap.put(prefix, 1);
                        } else {
                            freqMap.put(prefix, freq + 1);
                        }
                    }
                    for (String prefix : freqMap.keySet()) {
                        Tuple2<String, Tuple2<Set<Character>, Integer>> tuple2 = new Tuple2<>(prefix, new Tuple2<>(nextCharMap.get(prefix),
                                freqMap.get(prefix)));
                        result.add(tuple2);
                    }
                    return result;
                }
            });
            JavaPairRDD<String, Tuple2<Set<Character>, Integer>> pairRDD = locRDD.flatMapToPair(new PairFlatMapFunction<Set<Tuple2<String, Tuple2<Set<Character>, Integer>>>, String, Tuple2<Set<Character>, Integer>>() {
                @Override
                public Iterable<Tuple2<String, Tuple2<Set<Character>, Integer>>> call(Set<Tuple2<String, Tuple2<Set<Character>, Integer>>> tuple2s) throws Exception {
                    return tuple2s;
                }
            });
            JavaPairRDD<String, Tuple2<Set<Character>, Integer>> reducedRDD = pairRDD.combineByKey(new Function<Tuple2<Set<Character>, Integer>, Tuple2<Set<Character>, Integer>>() {
                @Override
                public Tuple2<Set<Character>, Integer> call(Tuple2<Set<Character>, Integer> _1) throws Exception {
                    return _1;
                }
            }, new Function2<Tuple2<Set<Character>, Integer>, Tuple2<Set<Character>, Integer>, Tuple2<Set<Character>, Integer>>() {
                @Override
                public Tuple2<Set<Character>, Integer> call(Tuple2<Set<Character>, Integer> _1, Tuple2<Set<Character>, Integer> _2) throws Exception {
                    Set<Character> set = new HashSet<>(_1._1);
                    set.addAll(_2._1);
                    return new Tuple2<>(set, _1._2 + _2._2);
                }
            }, new Function2<Tuple2<Set<Character>, Integer>, Tuple2<Set<Character>, Integer>, Tuple2<Set<Character>, Integer>>() {
                @Override
                public Tuple2<Set<Character>, Integer> call(Tuple2<Set<Character>, Integer> _1, Tuple2<Set<Character>, Integer> _2) throws Exception {
                    _1._1.addAll(_2._1);
                    return new Tuple2<>(_1._1, _1._2 + _2._2);
                }
            });
            Map<String, Tuple2<Set<Character>, Integer>> map = reducedRDD.collectAsMap();

            for (String prefix : map.keySet()) {
                Tuple2<Set<Character>, Integer> tuple2 = map.get(prefix);
                Set<Character> piNextList = new HashSet<>(tuple2._1.size());
                for (Character character : tuple2._1) {
                    if (isTerminator(character)) { //S中任意一个串以pi结尾
                        piTerminator.put(prefix, true);
                    } else {
                        piNextList.add(character);//加入pi下一个字符(不包括终结符)
                    }
                }
                currentFpiList.put(prefix, tuple2._2);//统计pi频率
                piNext.put(prefix, piNextList);
            }

            List<String> newP_ = new ArrayList<>();
            for (String pi : P_) {
                String piWithoutSplitter = noSplitter(pi);
                int fpi = currentFpiList.get(piWithoutSplitter);
                if (fpi > 0 && fpi <= Fm) {
                    P.add(pi);
                    fpiList.put(pi, fpi);
                } else if (fpi > Fm) {
                    //如果prefix的下一个字符是终结符
                    if (piTerminator.get(piWithoutSplitter)) {
                        boolean first = false;
                        for (Character c : piNext.get(piWithoutSplitter)) {
                            if (!first) {
                                newP_.add(pi + SPLITTER_INSERTION + c);
                                first = true;
                                pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                            } else {
                                newP_.add(pi + SPLITTER + c);
                            }
                        }
                    } else {//prefix的下一个不是终结符
                        //如果prefix的下一个字符只有一种，则直接扩展
                        if (piNext.get(piWithoutSplitter).size() == 1) {
                            for (Character c : piNext.get(piWithoutSplitter)) {
                                newP_.add(pi + c);
                            }
                        } else {//如果prefix的下一个字符有多种
                            boolean first = false;
                            for (Character c : piNext.get(piWithoutSplitter)) {
                                if (!first) {
                                    newP_.add(pi + SPLITTER + c);
                                    first = true;
                                    pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                                } else {
                                    newP_.add(pi + SPLITTER + c);
                                }
                            }
                        }
                    }
                }
            }
            P_ = newP_;
        }
        //sort P in decending fpi order
        P = new ArrayList<>(fpiList.keySet());
        Collections.sort(P, new Comparator<String>() {
            public int compare(String o1, String o2) {
                if (fpiList.get(o1) > fpiList.get(o2))
                    return -1;
                else if (fpiList.get(o1).equals(fpiList.get(o2)))
                    return 0;
                else return 1;
            }
        });
        ////////////////////////
        do {
            Set<String> G = new HashSet<>();
            //add P.head to G and remove the item from P
            G.add(P.remove(0));
            for (int i = 0; i < P.size(); i++) {
                String sCurr = P.get(i);
                int sumG = 0;
                for (String gi : G) {
                    sumG += fpiList.get(gi);
                }
                if (fpiList.get(sCurr) + sumG <= Fm) {
                    //add curr to G and remove the item from P
                    G.add(sCurr);
                    P.remove(i);
                    i--;
                }
            }
            virtualTree.add(G);
        } while (!P.isEmpty());
        return virtualTree;
    }

    /**
     * 垂直分区
     *
     * @param S        字符串列表
     * @param alphabet 字母表
     * @param Fm       参数Fm
     * @return 返回集合列表，每个元素是集合，集合内容是pi
     */
    Set<Set<String>> verticalPartitioning(List<String> S, Set<Character> alphabet, int Fm) {
        Set<Set<String>> virtualTree = new HashSet<>();
        List<String> P_ = new ArrayList<>();
        List<String> P = new ArrayList<>();
        final Map<String, Integer> fpiList = new HashMap<>();
        //每个key一个队列

        //如果c是原生类型，就用+""转换，如果c是包装类型，就用toString
        for (Character s : alphabet)
            P_.add(s.toString());
        //////////////
        while (!P_.isEmpty()) {
            final int P_Size = P_.size();
            Map<String, Integer> currentFpiList = new HashMap<>(P_Size);
            //当pi对应的Set长度大于1，则需要拆分插入，频率为0跳过该pi，频率为1直接扩展一个字符
            final Map<String, Set<Character>> piNext = new HashMap<>(P_Size);//key pi value 下一个字符（不包括终结符）
            Map<String, Boolean> piTerminator = new HashMap<>(P_Size);//key pi value pi下一个字符是否是终结符
            final List<String> prefixList = new ArrayList<>(P_Size);
            for (String pi : P_) {//对每个pi
                String piWithoutSplitter = noSplitter(pi);
                piTerminator.put(piWithoutSplitter, false);
                prefixList.add(piWithoutSplitter);
            }
            //Map<String,Tuple2<Character,Integer>> 前缀，前缀下一个字符集合，出现频率
            //String,Tuple2<Character,Integer> //(prefix ，(下一个字符，1))
            //对于每个串，找字串集合中的元素在该串中是否存在
            Map<String, Set<Character>> prefixNextChar = new HashMap<>();
            Map<String, Integer> prefixFreq = new HashMap<>();

            for (String line : S) {
                Map<String, Integer> freqMap = new HashMap<>();
                Map<String, Set<Character>> nextCharMap = new HashMap<>();
                Trie trie = Trie.builder().addKeywords(prefixList).build();

                Collection<Emit> piList = trie.parseText(line);

                for (Emit piWithoutSplitter : piList) {
                    if (piWithoutSplitter.getEnd() >= line.length() - 1) {
                        System.out.println(piWithoutSplitter.getEnd() + "/" + line.length());
                    }
                    Character nextChar = line.charAt(piWithoutSplitter.getEnd() + 1);
                    String prefix = piWithoutSplitter.getKeyword();
                    Set<Character> nextCharSet = nextCharMap.get(prefix);

                    if (nextCharSet == null) {
                        nextCharSet = new HashSet<>();
                        nextCharMap.put(prefix, nextCharSet);
                    }
                    nextCharSet.add(nextChar);
                    Integer freq = freqMap.get(prefix);
                    if (freq == null) {
                        freqMap.put(prefix, 1);
                    } else {
                        freqMap.put(prefix, freq + 1);
                    }
                }

                for (String prefix : freqMap.keySet()) {
                    Set<Character> nextCharSet = prefixNextChar.get(prefix);
                    Integer freq = prefixFreq.get(prefix);

                    if (nextCharSet == null) {
                        nextCharSet = new HashSet<>();
                        prefixNextChar.put(prefix, nextCharSet);
                        freq = 0;
                    }

                    nextCharSet.addAll(nextCharMap.get(prefix));
                    freq += freqMap.get(prefix);
                    prefixFreq.put(prefix, freq);
                }
            }

            for (Map.Entry<String, Set<Character>> entry : prefixNextChar.entrySet()) {
                String prefix = entry.getKey();
                Set<Character> piNextList = entry.getValue();
                Integer freq = prefixFreq.get(prefix);

                for (Character character : piNextList) {
                    if (isTerminator(character)) { //S中任意一个串以pi结尾
                        piTerminator.put(prefix, true);
                    } else {
                        piNextList.add(character);//加入pi下一个字符(不包括终结符)
                    }
                }
                currentFpiList.put(prefix, freq);//统计pi频率
                piNext.put(prefix, piNextList);
            }

            List<String> newP_ = new ArrayList<>();

            for (String pi : P_) {
                String piWithoutSplitter = noSplitter(pi);
                int fpi = currentFpiList.get(piWithoutSplitter);
                if (fpi > 0 && fpi <= Fm) {
                    P.add(pi);
                    fpiList.put(pi, fpi);
                } else if (fpi > Fm) {
                    //如果prefix的下一个字符是终结符
                    if (piTerminator.get(piWithoutSplitter)) {
                        boolean first = false;
                        for (Character c : piNext.get(piWithoutSplitter)) {
                            if (!first) {
                                newP_.add(pi + SPLITTER_INSERTION + c);
                                first = true;
                                pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                            } else {
                                newP_.add(pi + SPLITTER + c);
                            }
                        }
                    } else {//prefix的下一个不是终结符
                        //如果prefix的下一个字符只有一种，则直接扩展
                        if (piNext.get(piWithoutSplitter).size() == 1) {
                            for (Character c : piNext.get(piWithoutSplitter)) {
                                newP_.add(pi + c);
                            }
                        } else {//如果prefix的下一个字符有多种
                            boolean first = false;
                            for (Character c : piNext.get(piWithoutSplitter)) {
                                if (!first) {
                                    newP_.add(pi + SPLITTER + c);
                                    first = true;
                                    pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                                } else {
                                    newP_.add(pi + SPLITTER + c);
                                }
                            }
                        }
                    }
                }
            }
            P_ = newP_;
        }
        //sort P in decending fpi order
        P = new ArrayList<>(fpiList.keySet());
        P.sort((o1, o2) -> {
            if (fpiList.get(o1) > fpiList.get(o2))
                return -1;
            else if (fpiList.get(o1).equals(fpiList.get(o2)))
                return 0;
            else return 1;
        });
        ////////////////////////
        do {
            Set<String> G = new HashSet<>();
            //add P.head to G and remove the item from P
            G.add(P.remove(0));
            for (int i = 0; i < P.size(); i++) {
                String sCurr = P.get(i);
                int sumG = 0;
                for (String gi : G) {
                    sumG += fpiList.get(gi);
                }
                if (fpiList.get(sCurr) + sumG <= Fm) {
                    //add curr to G and remove the item from P
                    G.add(sCurr);
                    P.remove(i);
                    i--;
                }
            }
            virtualTree.add(G);
        } while (!P.isEmpty());
        return virtualTree;
    }

    Set<Set<String>> verticalPartitioning1(List<String> S, Set<Character> alphabet, int Fm) {
        Set<Set<String>> virtualTree = new HashSet<>();
        List<String> P_ = new ArrayList<>();
        List<String> P = new ArrayList<>();
        final Map<String, Integer> fpiList = new HashMap<>();
        //每个key一个队列

        //如果c是原生类型，就用+""转换，如果c是包装类型，就用toString
        for (Character s : alphabet)
            P_.add(s.toString());

        //////////////
        while (!P_.isEmpty()) {
            int P_Size = P_.size();
            List<String> prefixWithoutSplitterList = new ArrayList<>(P_Size);
            Map<String, Integer> currentFpiList = new HashMap<>(P_Size);
            //当pi对应的Set长度大于1，则需要拆分插入，频率为0跳过该pi，频率为1直接扩展一个字符
            Map<String, Set<Character>> piNext = new HashMap<>(P_Size);//key pi value 下一个字符（不包括终结符）
            Map<String, Boolean> piTerminator = new HashMap<>(P_Size);//key pi value pi下一个字符是否是终结符

            for (String pi : P_) {//对每个pi
//                String piWithoutSplitter = pi.replace(SPLITTER + "", "").replace(SPLITTER_INSERTION + "", "");
                String piWithoutSplitter = noSplitter(pi);
                prefixWithoutSplitterList.add(piWithoutSplitter);
                currentFpiList.put(piWithoutSplitter, 0);
                piNext.put(piWithoutSplitter, new HashSet<Character>());
                piTerminator.put(piWithoutSplitter, false);
            }
            Trie trie = Trie.builder().addKeywords(prefixWithoutSplitterList).build();

            for (String text : S) {//遍历主串，寻找所有的pi
                Collection<Emit> piList = trie.parseText(text);
                for (Emit piWithoutSplitter : piList) {
                    String sPiWithoutSplitter = piWithoutSplitter.getKeyword();
                    int end = piWithoutSplitter.getEnd() + 1;
                    Set<Character> piNextList = piNext.get(sPiWithoutSplitter);//pi下一个字符集合
                    if (isTerminator(text.charAt(end))) { //S中任意一个串以pi结尾
                        piTerminator.put(sPiWithoutSplitter, true);
                    } else {
                        piNextList.add(text.charAt(end));//加入pi下一个字符(不包括终结符)
                    }
                    currentFpiList.put(sPiWithoutSplitter, currentFpiList.get(sPiWithoutSplitter) + 1);//统计pi频率
                }
            }
            List<String> newP_ = new ArrayList<>();
            for (String pi : P_) {
//                String piWithoutSplitter = pi.replace(SPLITTER + "", "").replace(SPLITTER_INSERTION + "", "");
                String piWithoutSplitter = noSplitter(pi);
                int fpi = currentFpiList.get(piWithoutSplitter);
                if (fpi > 0 && fpi <= Fm) {
                    P.add(pi);
                    fpiList.put(pi, fpi);
                } else if (fpi > Fm) {
                    //如果prefix的下一个字符是终结符
                    if (piTerminator.get(piWithoutSplitter)) {
                        boolean first = false;
                        for (Character c : piNext.get(piWithoutSplitter)) {
                            if (!first) {
                                newP_.add(pi + SPLITTER_INSERTION + c);
                                first = true;
                                pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                            } else {
                                newP_.add(pi + SPLITTER + c);
                            }
                        }
                    } else {//prefix的下一个不是终结符
                        //如果prefix的下一个字符只有一种，则直接扩展
                        if (piNext.get(piWithoutSplitter).size() == 1) {
                            for (Character c : piNext.get(piWithoutSplitter)) {
                                newP_.add(pi + c);
                            }
                        } else {//如果prefix的下一个字符有多种
                            boolean first = false;
                            for (Character c : piNext.get(piWithoutSplitter)) {
                                if (!first) {
                                    newP_.add(pi + SPLITTER + c);
                                    first = true;
                                    pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                                } else {
                                    newP_.add(pi + SPLITTER + c);
                                }
                            }
                        }
                    }
                }
            }
            P_ = newP_;
        }
        //sort P in decending fpi order
        P = new ArrayList<>(fpiList.keySet());
        Collections.sort(P, new Comparator<String>() {
            public int compare(String o1, String o2) {
                if (fpiList.get(o1) > fpiList.get(o2))
                    return -1;
                else if (fpiList.get(o1).equals(fpiList.get(o2)))
                    return 0;
                else return 1;
            }
        });
        ////////////////////////
        do {
            Set<String> G = new HashSet<>();
            //add P.head to G and remove the item from P
            G.add(P.remove(0));
            for (int i = 0; i < P.size(); i++) {
                String sCurr = P.get(i);
                int sumG = 0;
                for (String gi : G) {
                    sumG += fpiList.get(gi);
                }
                if (fpiList.get(sCurr) + sumG <= Fm) {
                    //add curr to G and remove the item from P
                    G.add(sCurr);
                    P.remove(i);
                    i--;
                }
            }
            virtualTree.add(G);
        } while (!P.isEmpty());
        return virtualTree;
    }

    private class RPLComparator implements Comparator<RPL>, Serializable {
        /*
         * if s1>s2 return 1 else if s1<s2 return -1 else return 0
         * */
        int myCompare(String s1, String s2) {
            int end = Math.min(s1.length(), s2.length());
            for (int i = 0; i < end; i++) {
                char c1 = s1.charAt(i);
                char c2 = s2.charAt(i);
                if (c1 != c2) {
                    if (isTerminator(c1) && !isTerminator(c2))
                        return 1;
                    else if (!isTerminator(c1) && isTerminator(c2))
                        return -1;
                    else if (isTerminator(c1) && isTerminator(c2) || !isTerminator(c1) && !isTerminator(c2))
                        return c1 > c2 ? 1 : -1;
                }
            }
            return 0;
        }

        public int compare(RPL o1, RPL o2) {
            return myCompare(o1.R, o2.R);
        }
    }

    private class RPL implements Serializable {
        String R;
        int P;
        int L;//第index串的起始位置
        int index;//第几个串

        RPL(int index, int L) {
            this.index = index;
            this.L = L;
        }
    }

    private final RPLComparator RPLCOMPARATOR = new RPLComparator();

    /**
     * @param S         字符串列表
     * @param prefixSet 前缀集合(包含拆分符)
     * @return 返回(key包含拆分符的前缀 value前缀在串的位置)
     */
    Map<String, List<int[]>> getPrefixLoc(List<String> S, Set<String> prefixSet) {
        Map<String, List<int[]>> prefixLoc = new HashMap<>(prefixSet.size());//<prefixWithoutSplitter, Prefix Location>
        Map<String, String> prefixMap = new HashMap<>(prefixSet.size());//<prefixWithoutSplitter, Prefix>
        for (String prefix : prefixSet) {
            String prefixWithoutSplitter = noSplitter(prefix);
            prefixMap.put(prefixWithoutSplitter, prefix);
        }

        AhoCorasickDoubleArrayTrie<String> acdat = new AhoCorasickDoubleArrayTrie<>();
        acdat.build(prefixMap);
        for (int index = 0; index < S.size(); index++) {//遍历主串，寻找所有的pi
            String line = S.get(index);
            List<AhoCorasickDoubleArrayTrie.Hit<String>> prefixList = acdat.parseText(line);
            for (AhoCorasickDoubleArrayTrie.Hit<String> prefixInfo : prefixList) {
                String sPrefix = prefixInfo.value;
                List<int[]> locList = prefixLoc.get(sPrefix);
                if (locList == null) {
                    locList = new ArrayList<>();
                    prefixLoc.put(sPrefix, locList);
                }
                locList.add(new int[]{index, prefixInfo.begin});//寻找prefix的起始位置
            }
        }
        return prefixLoc;
    }

    /**
     * 子树准备
     *
     * @param S       字符串列表
     * @param prefix  垂直分区产生的prefix(包含拆分符)
     * @param locList 拆分符在住主串S中的位置
     * @return 返回L_B结构，L是后缀起始位置，B是分支信息
     */
    L_B subTreePrepare(List<String> S, String prefix, List<int[]> locList) {
        List<RPL> RPLList = new ArrayList<>(locList.size());
        String prefixWithoutSplitter = noSplitter(prefix);
        int start = prefixWithoutSplitter.length();//去掉分割标记
        //初始化L集合，即前缀prefix在主串S中的位置
        for (int[] loc : locList) {
            RPLList.add(new RPL(loc[0], loc[1]));
        }

        int LSize = RPLList.size();
        int remainingLeaves = LSize;//剩余待处理的R
        int[] B = new int[LSize];
        boolean[] Adone = new boolean[LSize];
        int[] I = new int[LSize];
        List<Integer> initAAIndex = new ArrayList<>(LSize);//活动区0对应的编号列表
        Map<Integer, List<Integer>> AAList = new HashMap<>();//key为活动区id，value为同一个活动区的index
        List<Integer> undefinedB = new ArrayList<>(LSize);//未定义的B
        for (int i = 0; i < LSize; i++) {
            I[i] = i;
            RPLList.get(i).P = i;
            undefinedB.add(i);
            initAAIndex.add(i);//0号活动区对应的ID
        }
        AAList.put(0, initAAIndex);//添加0号活动区
        undefinedB.remove(0);
        //一开始只有0号活动区，0号活动区的元素为0-len-1
        int lastActiveAreaId = 0;
        while (true) {
            if (undefinedB.size() == 0)
                break;

            int range = getRangeOfSymbols(remainingLeaves);//line 9
            for (int i = 0; i < LSize; i++) {//line 10
                if (I[i] != -1) {
                    //R[I[i]]=READRANGE(S,L[I[i]]+start,range)
                    RPL rpl = RPLList.get(I[i]);
                    int index = rpl.index;
                    int L = rpl.L;
                    int begin = L + start;
                    int end = begin + range;
                    String string = S.get(index);
                    if (end > string.length())
                        end = string.length();
                    rpl.R = string.substring(begin, end);
                }
            }
            ////////////Line 13-Line 15 START/////////////
            Map<Integer, List<Integer>> newAAList = new HashMap<>(AAList);
            for (Integer AAId : AAList.keySet()) {//遍历每个活动区
                //如果活动区中有任意被done的（我猜一个done则该活动区所有元素都done）
                List<Integer> indexList = AAList.get(AAId);//AAId活动区拥有的index
                if (Adone[indexList.get(0)])
                    continue;
                List<RPL> subRPLList = new ArrayList<>(indexList.size());
                for (Integer index : indexList)//将该活动区的所有元素copy到subRPLList
                    subRPLList.add(RPLList.get(index));
                Collections.sort(subRPLList, RPLCOMPARATOR);//对subRPLList进行排序
                for (int i = 0; i < indexList.size(); i++) {
                    int index = indexList.get(i);
                    RPL rpl = subRPLList.get(i);
                    I[rpl.P] = index;//maintain I
                    RPLList.set(index, rpl);//把排序好的RPL回填
                }
                for (int i = 0; i < indexList.size(); ) {
                    int index = indexList.get(i);
                    int j = i + 1;
                    while (j < indexList.size()) {
                        int indexJ = indexList.get(j);
                        if (!RPLList.get(index).R.equals(RPLList.get(indexJ).R))
                            break;
                        j++;
                    }
                    if (j != i + 1) {//发现活动区
                        List<Integer> newAA = new ArrayList<>(j - i);
                        for (int k = i; k < j; k++) {//将R相同的元素从旧活动区移除，加入新的活动区
                            newAA.add(indexList.remove(i));
                            if (indexList.size() == 0)
                                newAAList.remove(AAId);
                        }
                        newAAList.put(++lastActiveAreaId, newAA);
                    } else
                        i = j;
                }
            }
            AAList = newAAList;
            ////////////Line 13-Line 15 END/////////////
            for (int ind = undefinedB.size() - 1; ind >= 0; ind--) {
                int i = undefinedB.get(ind);
                //B[i] is not defined
                //cs is the common S-prefix of R[i-1] and R[i]
                int cs = 0;
                String R1 = RPLList.get(i - 1).R;
                String R2 = RPLList.get(i).R;
                for (int j = 0; j < Math.min(R1.length(), R2.length()); j++) {
                    //R1与R2的字符相等
                    if (R1.charAt(j) == R2.charAt(j)) {
                        cs++;
                    } else
                        break;
                }
                if (cs < range) {//line 18
                    //line 19
                    B[i] = start + cs;
                    undefinedB.remove(ind);
                    if (B[i - 1] > 0 || i == 1) {
                        I[RPLList.get(i - 1).P] = -1;
                        Adone[i - 1] = true;
                        RPLList.get(i - 1).R = null;
                        remainingLeaves--;
                    }
                    if (i == RPLList.size() - 1 || B[i + 1] > 0) {
                        I[RPLList.get(i).P] = -1;
                        Adone[i] = true;
                        RPLList.get(i).R = null;
                        remainingLeaves--;
                    }
                }
            }
            start += range;
        }
        List<int[]> newL = new ArrayList<>(RPLList.size());
        for (RPL rpl : RPLList)
            newL.add(new int[]{rpl.index, rpl.L});
        return new L_B(newL, B);
    }

    /**
     * 构建子树
     *
     * @param lb 子树准备返回的L,B
     * @return 返回树的根节点
     */
    TreeNode buildSubTree(List<String> S, L_B lb) {
        TreeNode root = new TreeNode();
        TreeNode u_ = new TreeNode();
        root.parent = null;
        root.leftChild = u_;
        u_.parent = root;
        List<int[]> L = lb.getL();
        int[] B = lb.getB();
        //L-B只有一个元素
        int[] L0 = L.get(0);
        u_.index = L0[0];
        u_.start = L0[1];
        u_.end = S.get(L0[0]).length();
        u_.suffix_index = L0[1];

        Stack<TreeNode> stack = new Stack<>();
        stack.push(u_);
        Map<TreeNode, Integer> v1Length = new HashMap<>();//从根节点到某一结点走过的字符串的长度
        v1Length.put(root, 0);
        int depth = u_.end - u_.start;
        for (int i = 1; i < B.length; i++) {
            int offset = B[i];//公共前缀长度common
            TreeNode v1, v2, u;
            do {
                TreeNode se = stack.pop();
                v1 = se.parent;
                v2 = se;
                depth -= se.end - se.start;
            } while (depth > offset);
            if (depth == offset) {
                u = v1.leftChild;
            } else {
                //寻找根节点到v1经历了几个字符
                int before = v1Length.get(v1);
                int end = offset - before;//跳过根节点到v1这么长的字符

                TreeNode oldV2 = v2.clone();
                TreeNode vt = v2;
                vt.end = vt.start + end;//前半部分(v1,vt)字符串
                vt.suffix_index = -1;
                v2 = new TreeNode(oldV2.index, vt.end, oldV2.end);//(vt,v2)后半部分字符串
                //vt原来的左子树交给v2
                if (vt.leftChild != null) {
                    v2.leftChild = vt.leftChild;
                    //修改v2左子树及左子树右兄弟的父节点
                    TreeNode next = v2.leftChild;
                    while (next != null) {
                        next.parent = v2;
                        next = next.rightSibling;
                    }
                }
                vt.leftChild = v2;
                v2.parent = vt;
                v2.suffix_index = oldV2.suffix_index;

                //根节点到拆分形成的节点经过的字符串就是offset
                v1Length.put(vt, offset);
                u = v2;
                stack.push(vt);
                depth += vt.end - vt.start;
            }
            //标记(u,u'),遇到终结符则停止
            //L.get(i)+offset到末尾，存在$0则不要了
            int[] Li = L.get(i);
            String sLi = S.get(Li[0]);//第Li[0]个字符串
            int start = Li[1] + offset;
            //计算start越界后，则只保留终结符
            u_ = new TreeNode(Li[0], start, sLi.length());
            TreeNode next = u;
            while (next.rightSibling != null)
                next = next.rightSibling;
            next.rightSibling = u_;
            u_.parent = v2.parent;
            u_.suffix_index = Li[1];
            stack.push(u_);
            depth += u_.end - u_.start;
        }
        return root;
    }

    /**
     * 新建节点，并让新建节点作为被拆节点的父节点
     *
     * @param S    字符串列表
     * @param p    垂直分区产生的pi
     * @param root 构建子树产生的根节点
     */
    void splitSubTree(List<String> S, String p, TreeNode root) {
        TreeNode currNode = root.leftChild;
        int lastSplit = 0;
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < p.length(); i++) {
            if (p.charAt(i) == SPLITTER || p.charAt(i) == SPLITTER_INSERTION) {
                TreeNode newNode = new TreeNode(
                        currNode.index, currNode.start, currNode.start + (i - lastSplit));
                currNode.start = newNode.end;
                currNode.parent.leftChild = newNode;
                newNode.parent = currNode.parent;
                newNode.leftChild = currNode;
                currNode.parent = newNode;
                //将currNode的兄弟转移给上层
                newNode.rightSibling = currNode.rightSibling;
                currNode.rightSibling = null;
                String s = S.get(newNode.index);
                path.append(s.substring(newNode.start, newNode.end));
                lastSplit = i + 1;
                if (p.charAt(i) == SPLITTER_INSERTION) {
                    TreeNode sibling = currNode;
                    while (sibling.rightSibling != null) {
                        sibling = sibling.rightSibling;
                    }
                    for (int j = 0; j < S.size(); j++) {
                        String line = S.get(j);
                        //对于以path结尾的串，则插入叶节点
                        String replaceTerminator = line.substring(0, line.length() - 1);
                        if (replaceTerminator.endsWith(path.toString())) {
                            int start = line.lastIndexOf(path.toString());
                            TreeNode tmp = new TreeNode(j, start, line.length());
                            tmp.suffix_index = start;
                            sibling.rightSibling = tmp;
                            tmp.parent = sibling.parent;
                            sibling = tmp;
                        }
                    }
                }
            }
        }
    }

    /**
     * 遍历树，并打印所有叶节点
     *
     * @param root               树的根节点
     * @param terminatorFileName 终结符-文件名对应列表
     * @param result             返回一棵树所有叶节点遍历结果
     */
    void traverseTree(List<String> S, TreeNode root, Map<Character, String> terminatorFileName, Set<String> result) {
        Stack<TreeNode> stack = new Stack<>();
        TreeNode node = root;
        if (root == null)
            return;
        while (node != null || !stack.isEmpty()) {
            while (node != null) {
                stack.push(node);
                node = node.leftChild;
            }
            node = stack.pop();
            if (node.leftChild == null) {
                //result.add(String.format("%d %s:%d", stack.size(), terminatorFileName.get(S.get(node.index).charAt(node.end - 1)), node.suffix_index));
                String sb = stack.size() +
                        " " +
                        terminatorFileName.get(S.get(node.index).charAt(node.end - 1)) +
                        ":" +
                        node.suffix_index;
                result.add(sb);
            }
            node = node.rightSibling;
        }
    }


    /**
     * @param target The target string to compare with.
     * @param S      The String list
     * @param root   The tree root
     * @return return true if the target can be found, else return false.
     */
    public boolean endsWith(String target, List<String> S, ERA.TreeNode root) {
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
                    exists |= endsWith(target, S, root);
                }
            }
        } else { // if root is internal node
            if (target.startsWith(textOnNode))
                exists = endsWith(target.substring(textOnNode.length()), S, root.leftChild);
        }

        return exists;
    }
}
