package GST;

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

    private char terminator = 43000;//起始终结符
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
    char nextTerminator() {
        return terminator++;
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
    static Set<Character> getAlphabet(List<String> S) {
        Set<Character> alphabet = new HashSet<Character>();
        for (String line : S) {
            for (int j = 0; j < line.length() - 1; j++) {
                alphabet.add(line.charAt(j));
            }
        }
        return alphabet;
    }


    /**
     * 垂直分区
     *
     * @param S        字符串列表
     * @param alphabet 字母表
     * @param Fm       参数Fm
     * @return 返回集合列表，每个元素是集合，集合内容是pi
     */
    Set<Set<String>> verticalPartitioning(List<String> S, Set<Character> alphabet, long Fm) {
        Set<Set<String>> virtualTree = new HashSet<Set<String>>();
        List<String> P_ = new ArrayList<String>(alphabet.size());
        List<String> P = new ArrayList<String>(alphabet.size());
        //每个key一个队列
        Map<String, List<int[]>> rank = new HashMap<String, List<int[]>>();
        final Map<String, Long> fpiList = new HashMap<String, Long>();

        //如果c是原生类型，就用+""转换，如果c是包装类型，就用toString
        for (Character s : alphabet)
            P_.add(s.toString());

        //将下标i插入对应RankS[i]队列中

        for (int i = 0; i < S.size(); i++) {
            String line = S.get(i);
            //len-1跳过每行终结符
            for (int j = 0; j < line.length() - 1; j++) {
                String queueName = line.charAt(j) + "";
                List<int[]> queue = rank.get(queueName);
                if (queue == null) {
                    queue = new ArrayList<int[]>();
                    rank.put(queueName, queue);
                }
                int[] pos = new int[]{i, j};//i为第几个串，j为第i个串的起始位置
                queue.add(pos);
            }
        }

        //////////////
        while (!P_.isEmpty()) {
            String pi = P_.remove(0);//第一个元素总要被删，一开始就删了吧
            //SPLITTER+""转换成String要比new Character(SPLITTER).toString()快
            String piWithoutSplitter = pi.replace(SPLITTER + "", "").replace(SPLITTER_INSERTION + "", "");
            long fpi = rank.get(piWithoutSplitter).size();
            if (fpi > 0 && fpi <= Fm) {
                P.add(pi);
                fpiList.put(pi, fpi);
            } else if (fpi > Fm) {
                boolean _insert = false;

                for (Character s : alphabet) {
                    rank.put(piWithoutSplitter + s, new ArrayList<int[]>());
                }
                //这里j为RankPi的元素值
                List<int[]> piIndexes = rank.get(piWithoutSplitter);

                if (piIndexes.size() == 1) {
                    int[] index = piIndexes.get(0);
                    P_.add(pi + S.get(index[0]).charAt(index[1] + 1));
                } else {
                    for (int[] j : piIndexes) {
                        char id = S.get(j[0]).charAt(j[1] + 1);
                        //是终结符，把break置为true
                        if (isTerminator(id))
                            _insert = true;
                        else
                            rank.get(piWithoutSplitter + id).add(new int[]{j[0], j[1] + 1});
                    }
                    if (_insert) {
                        //解决拆分后重复的问题
                        boolean firstNoEmpty = false;
                        for (Character c : alphabet) {
                            String queueName = piWithoutSplitter + c;
                            //对于第一个非空队列，使用拆分、插入标记
                            //对于其他的使用拆分标记
                            if (rank.get(queueName).size() > 0) {
                                if (!firstNoEmpty) {
                                    P_.add(pi + SPLITTER_INSERTION + c);
                                    firstNoEmpty = true;
                                    //引入一个拆分插入符号后，修改pi，下次循环使用拆分符
                                    pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                                } else {
                                    //对于其他元素，使用拆分标记
                                    P_.add(pi + SPLITTER + c);
                                }
                            }
                        }
                    } else {
                        //修复部分节点多拆分的问题
                        int count = 0;
                        for (Character c : alphabet) {
                            if (rank.get(piWithoutSplitter + c).size() > 0) {
                                count++;
                                //优化
                                if (count > 1)
                                    break;
                            }
                        }
                        if (count > 1) {
                            boolean firstNoEmpty = false;
                            for (Character c : alphabet) {
                                String queueName = piWithoutSplitter + c;
                                if (rank.get(queueName).size() > 0) {
                                    //这里对于第一个非空使用SPLITTER_INSERTION，扩展其他使用SPLITTER
                                    if (!firstNoEmpty) {
                                        P_.add(pi + SPLITTER + c);
                                        firstNoEmpty = true;
                                        pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                                    } else {
                                        P_.add(pi + SPLITTER + c);
                                    }
                                }
                            }
                        } else {
                            for (Character c : alphabet) {
                                if (rank.get(piWithoutSplitter + c).size() > 0)
                                    P_.add(pi + c);
                            }
                        }
                    }
                }
            }
            rank.remove(piWithoutSplitter);
        }
        //sort P in decending fpi order
        P = new ArrayList<String>(fpiList.keySet());
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
            Set<String> G = new HashSet<String>();
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

    Set<Set<String>> verticalPartitioningAlpha(List<String> S, Set<Character> alphabet, long Fm, int maxLen) {
        Set<Set<String>> virtualTree = new HashSet<Set<String>>();
        List<String> P_ = new ArrayList<String>(alphabet.size());
        List<String> P = new ArrayList<String>(alphabet.size());
        //每个key一个队列
        Map<String, List<int[]>> rank = new HashMap<String, List<int[]>>();
        final Map<String, Long> fpiList = new HashMap<String, Long>();

        //如果c是原生类型，就用+""转换，如果c是包装类型，就用toString
        for (Character s : alphabet)
            P_.add(s.toString());

        //将下标i插入对应RankS[i]队列中

        for (int i = 0; i < S.size(); i++) {
            String line = S.get(i);
            //len-1跳过每行终结符
            for (int j = 0; j < line.length() - 1; j++) {
                String queueName = line.charAt(j) + "";
                List<int[]> queue = rank.get(queueName);
                if (queue == null) {
                    queue = new ArrayList<int[]>();
                    rank.put(queueName, queue);
                }
                int[] pos = new int[]{i, j};//i为第几个串，j为第i个串的起始位置
                queue.add(pos);
            }
        }

        //////////////
        while (!P_.isEmpty()) {
            String pi = P_.remove(0);//第一个元素总要被删，一开始就删了吧
            //SPLITTER+""转换成String要比new Character(SPLITTER).toString()快
            String piWithoutSplitter = pi.replace(SPLITTER + "", "").replace(SPLITTER_INSERTION + "", "");
            long fpi = rank.get(piWithoutSplitter).size();
            if (fpi > 0 && fpi <= Fm) {
                P.add(pi);
                fpiList.put(pi, fpi);
            } else if (fpi > Fm) {
                boolean _insert = false;

                for (Character s : alphabet) {
                    rank.put(piWithoutSplitter + s, new ArrayList<int[]>());
                }
                //这里j为RankPi的元素值
                List<int[]> piIndexes = rank.get(piWithoutSplitter);

                if (piIndexes.size() == 1) {
                    int[] index = piIndexes.get(0);
                    P_.add(pi + S.get(index[0]).charAt(index[1] + 1));
                } else {
                    for (int[] j : piIndexes) {
                        char id = S.get(j[0]).charAt(j[1] + 1);
                        //是终结符，把break置为true
                        if (isTerminator(id))
                            _insert = true;
                        else
                            rank.get(piWithoutSplitter + id).add(new int[]{j[0], j[1] + 1});
                    }
                    if (_insert) {
                        //解决拆分后重复的问题
                        boolean firstNoEmpty = false;
                        for (Character c : alphabet) {
                            String queueName = piWithoutSplitter + c;
                            //对于第一个非空队列，使用拆分、插入标记
                            //对于其他的使用拆分标记
                            if (rank.get(queueName).size() > 0) {
                                if (!firstNoEmpty) {
                                    P_.add(pi + SPLITTER_INSERTION + c);
                                    firstNoEmpty = true;
                                    //引入一个拆分插入符号后，修改pi，下次循环使用拆分符
                                    pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                                } else {
                                    //对于其他元素，使用拆分标记
                                    P_.add(pi + SPLITTER + c);
                                }
                            }
                        }
                    } else {
                        //修复部分节点多拆分的问题
                        int count = 0;
                        for (Character c : alphabet) {
                            if (rank.get(piWithoutSplitter + c).size() > 0) {
                                count++;
                                //优化
                                if (count > 1)
                                    break;
                            }
                        }
                        if (count > 1) {
                            boolean firstNoEmpty = false;
                            for (Character c : alphabet) {
                                String queueName = piWithoutSplitter + c;
                                if (rank.get(queueName).size() > 0) {
                                    //这里对于第一个非空使用SPLITTER_INSERTION，扩展其他使用SPLITTER
                                    if (!firstNoEmpty) {
                                        P_.add(pi + SPLITTER + c);
                                        firstNoEmpty = true;
                                        pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                                    } else {
                                        P_.add(pi + SPLITTER + c);
                                    }
                                }
                            }
                        } else {
                            for (Character c : alphabet) {
                                if (rank.get(piWithoutSplitter + c).size() > 0)
                                    P_.add(pi + c);
                            }
                        }
                    }
                }
            }
            rank.remove(piWithoutSplitter);
        }
        //sort P in decending fpi order
        P = new ArrayList<String>(fpiList.keySet());
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
            Set<String> G = new HashSet<String>();
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
                    if (G.size() >= maxLen)
                        break;
                }
            }
            virtualTree.add(G);
        } while (!P.isEmpty());
        return virtualTree;
    }


    /**
     * 子树准备
     *
     * @param S 字符串列表
     * @param p 垂直分区产生的pi
     * @return 返回一个数组Object[2], Object[0]装的是L，Object[1]装的是B
     */
    L_B subTreePrepare(List<String> S, String p) {
        class RPL {
            private String R;
            private int P;
            private int[] L;


            private RPL(int[] L) {
                this.L = L;
            }
        }
        Map<Integer, Boolean> I_done = new HashMap<Integer, Boolean>();
        List<RPL> RPLList = new ArrayList<RPL>();
        Map<Integer, List<Integer>> activeAreaList = new HashMap<Integer, List<Integer>>();//key为活动区id，value为同一个活动区的index

        p = p.replace(SPLITTER_INSERTION + "", "").replace(SPLITTER + "", "");//去掉分割标记
        int start = p.length();
        //Line 1:L contains the locations of S-prefix p in string S
        for (int i = 0; i < S.size(); i++) {
            String line = S.get(i);
            int index = line.indexOf(p);
            while (index != -1) {
                //初始化L
                RPLList.add(new RPL(new int[]{i, index}));
                index = line.indexOf(p, index + 1);
            }
        }

        int L_ = RPLList.size();
        int[] B = new int[RPLList.size()];
        int[] A = new int[RPLList.size()];
        boolean[] A_done = new boolean[RPLList.size()];
        int[] I = new int[RPLList.size()];
        for (int i = 0; i < RPLList.size(); i++) {
            I[i] = i;
            I_done.put(i, false);
            RPLList.get(i).P = i;
        }
        //一开始只有0号活动区，0号活动区的元素为0-len-1
        activeAreaList.put(0, new ArrayList<Integer>());
        for (int i = 0; i < A.length; i++)
            activeAreaList.get(0).add(i);
        int lastActiveAreaId = 0;
        while (true) {
            //line 8
            //假设B都定义了
            boolean defined = true;
            for (int i = 1; i < B.length; i++) {
                if (B[i] == 0) {
                    //存在未定义的B
                    defined = false;
                    break;
                }
            }
            //line 8
            //B都定义了
            if (defined)
                break;

            int range = 4;//getRangeOfSymbols(L_);//line 9
            for (int i : I_done.keySet()) {//line 10
                if (!I_done.get(i)) {
                    //R[I[i]]=READRANGE(S,L[I[i]]+start,range)
                    int[] L = RPLList.get(I[i]).L;
                    int begin = L[1] + start;
                    int end = begin + range;
                    String string = S.get(L[0]);
                    if (end > string.length())
                        end = string.length();
                    RPLList.get(I[i]).R = string.substring(begin, end);
                }
            }
            ////////////Line 13-Line 15 START/////////////
            //遍历每个活动区
            Map<Integer, List<Integer>> changedActiveAreaList = new HashMap<Integer, List<Integer>>(activeAreaList);
            List<RPL> beforeOrderedRPLList = new ArrayList<RPL>(RPLList);
            for (Integer aaId : activeAreaList.keySet()) {
                //已经置done的活动区可以跳过
                if (A_done[activeAreaList.get(aaId).get(0)])
                    continue;
                List<Integer> rplIndexes = activeAreaList.get(aaId);//找到同一个活动区元素的位置列表
                List<RPL> aaRPL = new ArrayList<RPL>(rplIndexes.size());//对同一个活动区，取出元素放入这里
                //遍历同一个活动区的每个元素，取出来加入新的列表
                for (Integer index : rplIndexes) {
                    aaRPL.add(RPLList.get(index));
                }
                //对取出来的RPL根据R排序
                Collections.sort(aaRPL, new Comparator<RPL>() {
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
                });
                //将排序好的RPL回填到RPLList中
                int i = 0;
                for (Integer index : rplIndexes) {
                    RPLList.set(index, aaRPL.get(i++));
                }
                i = 0;
                //寻找活动区
                while (i < rplIndexes.size()) {
                    int j = i + 1;
                    while (j < rplIndexes.size() && RPLList.get(rplIndexes.get(i)).R.equals(RPLList.get(rplIndexes.get(j)).R)) {
                        j++;
                    }
                    //发现活动区
                    if (j != i + 1) {
                        lastActiveAreaId++;
                        List<Integer> newActiveArea = new ArrayList<Integer>(j - i + 1);
                        changedActiveAreaList.put(lastActiveAreaId, newActiveArea);

                        for (int k = i; k < j; k++) {
                            int aaIndex = rplIndexes.remove(i);
                            newActiveArea.add(aaIndex);//下表会后窜，所以都是删i
                            A[aaIndex] = lastActiveAreaId;
                            if (rplIndexes.size() == 0)
                                changedActiveAreaList.remove(aaId);
                        }
                    } else {
                        i = j;
                    }
                }
            }
            activeAreaList = changedActiveAreaList;
            int[] newI = new int[I.length];
            for (int i = 0; i < I.length; i++) {
                int j = 0;
                while (j < I.length) {
                    if (beforeOrderedRPLList.get(i).P == RPLList.get(j).P)
                        break;
                    j++;
                }
                newI[i] = j;
            }
            I = newI;
            ////////////Line 13-Line 15 END////////
            for (int i = 1; i < B.length; i++) {
                //B[i] is not defined
                if (B[i] == 0) { //line 16
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
                        if (B[i - 1] > 0 || i == 1) {
                            I_done.put(RPLList.get(i - 1).P, true);
                            A_done[i - 1] = true;
                            RPLList.get(i - 1).R = null;
                            L_--;
                        }
                        if (i == RPLList.size() - 1 || B[i + 1] > 0) {
                            I_done.put(RPLList.get(i).P, true);
                            A_done[i] = true;
                            RPLList.get(i).R = null;
                            L_--;
                        }
                    }
                }
            }
            start += range;
        }

        List<int[]> newL = new ArrayList<int[]>(RPLList.size());
        for (RPL rpl : RPLList)
            newL.add(rpl.L);
        return new L_B(newL, B);
    }

    private final Comparator<RPL> RPLComparator = new Comparator<RPL>() {
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
    };

    private class RPL {
        String R;
        int P;
        int L;//第index串的起始位置
        int index;//第几个串

        RPL(int index, int L) {
            this.index = index;
            this.L = L;
        }
    }

    /**
     * 子树准备
     *
     * @param S 字符串列表
     * @param p 垂直分区产生的pi
     * @return 返回一个数组Object[2], Object[0]装的是L，Object[1]装的是B
     */
    L_B subTreePrepareAlpha(List<String> S, String p) {
        List<RPL> RPLList = new ArrayList<RPL>();
        p = p.replace(SPLITTER_INSERTION + "", "").replace(SPLITTER + "", "");//去掉分割标记
        int start = p.length();
        //Line 1:L contains the locations of S-prefix p in string S
        for (int index = 0; index < S.size(); index++) {//遍历所有串
            String line = S.get(index);
            int L = line.indexOf(p);//寻找后缀的的前缀起始位置作为leaf
            while (L != -1) {
                //初始化L
                RPLList.add(new RPL(index, L));
                L = line.indexOf(p, L + 1);
            }
        }

        int LSize = RPLList.size();
        int remainingLeaves = LSize;//剩余待处理的R
        int[] B = new int[LSize];
        boolean[] Adone = new boolean[LSize];
        int[] I = new int[LSize];
        List<Integer> initAAIndex = new ArrayList<Integer>(LSize);//活动区0对应的编号列表
        Map<Integer, List<Integer>> AAList = new HashMap<Integer, List<Integer>>();//key为活动区id，value为同一个活动区的index
        List<Integer> undefinedB = new ArrayList<Integer>(LSize);//未定义的B
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
            Map<Integer, List<Integer>> newAAList = new HashMap<Integer, List<Integer>>(AAList);
            for (Integer AAId : AAList.keySet()) {//遍历每个活动区
                //如果活动区中有任意被done的（我猜一个done则该活动区所有元素都done）
                List<Integer> indexList = AAList.get(AAId);//AAId活动区拥有的index
                if (Adone[indexList.get(0)])
                    continue;
                List<RPL> subRPLList = new ArrayList<RPL>(indexList.size());
                for (Integer index : indexList)//将该活动区的所有元素copy到subRPLList
                    subRPLList.add(RPLList.get(index));
                Collections.sort(subRPLList, RPLComparator);//对subRPLList进行排序
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
                        List<Integer> newAA = new ArrayList<Integer>(j - i);
                        for (int k = i; k < j; k++) {//将R相同的元素从旧活动区移除，加入新的活动区
                            newAA.add(indexList.remove(i));
                            if (indexList.size() == 0)
                                newAAList.remove(AAId);
                        }
                        if (newAA.size() == 0)
                            System.out.println();
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
        List<int[]> newL = new ArrayList<int[]>(RPLList.size());
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

        Stack<TreeNode> stack = new Stack<TreeNode>();
        stack.push(u_);
        Map<TreeNode, Integer> v1Length = new HashMap<TreeNode, Integer>();//从根节点到某一结点走过的字符串的长度
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
     * @return 返回一棵树所有叶节点遍历结果
     */
    void traverseTree(List<String> S, TreeNode root, Map<Character, String> terminatorFileName, Set<String> result) {
        Stack<TreeNode> stack = new Stack<TreeNode>();
        TreeNode node = root;
        if (root == null)
            return;
        while (node != null || !stack.isEmpty()) {
            while (node != null) {
                stack.push(node);
                node = node.leftChild;
            }
            node = stack.pop();
            if (node.leftChild == null)
                result.add(String.format("%d %s:%d", stack.size(), terminatorFileName.get(S.get(node.index).charAt(node.end - 1)), node.suffix_index));
            node = node.rightSibling;
        }
    }
}
