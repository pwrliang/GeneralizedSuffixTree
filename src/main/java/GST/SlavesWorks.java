package GST;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.spark.api.java.JavaRDD;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.*;

/**
 * Created by lib on 16-11-11.
 */
public class SlavesWorks {
    private char terminator = 43000;
    public static final char SPLITTER_INSERTION = 57001;
    public static final char SPLITTER = 57002;


    public static class TypeB {
        char c1, c2;
        int common;

        TypeB() {
        }

        TypeB(char c1, char c2, int common) {
            this.c1 = c1;
            this.c2 = c2;
            this.common = common;
        }
    }

    public static class TreeNode {
        String data;
        int[] index;
        TreeNode parent;
        TreeNode leftChild;
        TreeNode rightSibling;

        private TreeNode() {
        }

        private TreeNode(String data, int[] index) {
            this.data = data;
            this.index = index;
        }
    }

    private boolean isTerminator(char c) {
        return c >= 43000 && c <= 57000;
    }

    public char nextTerminator() {
        return terminator++;
    }

    private int getRangeOfSymbols() {
        return 200;
    }

    public Set<Character> getAlphabet(List<String> S) {
        Set<Character> alphabet = new HashSet<Character>();
        for (String line : S) {
            for (int j = 0; j < line.length() - 1; j++) {
                alphabet.add(line.charAt(j));
            }
        }
        return alphabet;
    }

    //对原算法的改进，标记了需要拆分的叶节点
    //二位数组版本
    public Set<Set<String>> verticalPartitioning(List<String> S, Set<Character> alphabet, int Fm) {
        Set<Set<String>> virtualTree = new HashSet<Set<String>>();
        List<String> P_ = new LinkedList<String>();
        List<String> P = new LinkedList<String>();
        //每个key一个队列
        Map<String, List<int[]>> rank = new HashMap<String, List<int[]>>();
        final Map<String, Integer> fpiList = new HashMap<String, Integer>();

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
            int fpi = rank.get(piWithoutSplitter).size();

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
                            if (rank.get(queueName).size() > 0 && !firstNoEmpty) {
                                P_.add(pi + SPLITTER_INSERTION + c);
                                firstNoEmpty = true;
                                //引入一个拆分插入符号后，修改pi，下次循环使用拆分符
                                pi = pi.replace(SPLITTER_INSERTION, SPLITTER);
                            } else {
                                //对于其他元素，使用拆分标记
                                if (rank.get(piWithoutSplitter + c + "").size() > 0)
                                    P_.add(pi + SPLITTER + c);

                            }
                        }
                    } else {
                        for (Character c : alphabet) {
                            P_.add(pi + SPLITTER + c);
                        }
                    }
                }
            }
            rank.remove(piWithoutSplitter);
        }
        //sort P in decending fpi order
        P = new LinkedList<String>(fpiList.keySet());
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
            //源代码把p头移除放到G中，这里把p尾部移除放到G中
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


    private List<String> S;
    private Set<String> p;
    private Map<Character, String> terminatorFilename;

    public SlavesWorks() {

    }

    public SlavesWorks(List<String> S, Set<String> p, Map<Character, String> terminatorFilename) {
        this.S = S;
        this.p = p;
        this.terminatorFilename = terminatorFilename;
    }

    public List<String> work() {
        for (String Pi : p) {
            Object[] L_B = subTreePrepare(S, Pi);
            TreeNode root = buildSubTree((List<int[]>) L_B[0], (List<TypeB>) L_B[1]);
            splitSubTree(S, Pi, root);
            traverseTree(root, terminatorFilename);
        }
        return result;
    }

    //二维版本的subTreePrepare
    public Object[] subTreePrepare(List<String> S, String p) {
        class RPL {
            private String R;
            private int P;
            private int[] L;


            private RPL(int[] L) {
                this.L = L;
            }
        }
        List<TypeB> B = new ArrayList<TypeB>();
        List<Boolean> B_defined = new ArrayList<Boolean>();
        List<Integer> I = new ArrayList<Integer>();
        Map<Integer, Boolean> I_done = new HashMap<Integer, Boolean>();
        List<Integer> A = new ArrayList<Integer>();
        List<Boolean> A_done = new ArrayList<Boolean>();
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

        for (int i = 0; i < RPLList.size(); i++) {
            B.add(new TypeB());
            B_defined.add(false);
            I.add(i);
            I_done.put(i, false);
            A.add(0);
            A_done.add(false);
            RPLList.get(i).P = i;
        }
        //一开始只有0号活动区，0号活动区的元素为0-len-1
        activeAreaList.put(0, new ArrayList<Integer>());
        for (int i = 0; i < A.size(); i++)
            activeAreaList.get(0).add(i);

        while (true) {
            //line 8
            //假设B都定义了
            boolean defined = true;
            for (int i = 1; i < B_defined.size(); i++) {
                if (!B_defined.get(i)) {
                    //存在未定义的B
                    defined = false;
                    break;
                }
            }
            //line 8
            //B都定义了
            if (defined)
                break;

            int range = getRangeOfSymbols();//line 9
            for (int i = 0; i < RPLList.size(); i++) {//line 10
                if (!I_done.get(I.get(i))) {
                    //R[I[i]]=READRANGE(S,L[I[i]]+start,range)
                    int[] L = RPLList.get(I.get(i)).L;
                    int begin = L[1] + start;
                    int end = begin + range;
                    if (end > S.get(L[0]).length())
                        end = S.get(L[0]).length();
                    RPLList.get(I.get(i)).R = S.get(L[0]).substring(begin, end);
                }
            }
            //此部分伪代码不明确，我写的可能有问题
            ////////////Line 13-Line 15 START/////////////
            //遍历每个活动区
            int lastActiveAreaId = 0;
            //遍历活动区找到最大的aaID
            for (Integer aaId : activeAreaList.keySet())
                if (aaId > lastActiveAreaId) {
                    lastActiveAreaId = aaId;
                }
            Map<Integer, List<Integer>> changedActiveAreaList = new HashMap<Integer, List<Integer>>(activeAreaList);
            List<RPL> beforeOrderedRPLList = new ArrayList<RPL>(RPLList);
            for (Integer aaId : activeAreaList.keySet()) {
                //已经置done的活动区可以跳过
                if (A_done.get(activeAreaList.get(aaId).get(0)))
                    continue;
                List<Integer> rplIndexes = activeAreaList.get(aaId);//找到同一个活动区元素的位置列表
                List<RPL> aaRPL = new LinkedList<RPL>();//对同一个活动区，取出元素放入这里
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
                        List<Integer> newActiveArea = activeAreaList.get(lastActiveAreaId);
                        if (newActiveArea == null) {
                            newActiveArea = new ArrayList<Integer>();
                            changedActiveAreaList.put(lastActiveAreaId, newActiveArea);
                        }
                        for (int k = i; k < j; k++) {
                            int aaIndex = rplIndexes.remove(i);
                            newActiveArea.add(aaIndex);//下表会后窜，所以都是删i
                            A.set(aaIndex, lastActiveAreaId);
                            if (rplIndexes.size() == 0)
                                changedActiveAreaList.remove(aaId);
                        }
                    } else {
                        i = j;
                    }
                }
            }
            activeAreaList = changedActiveAreaList;
            List<Integer> newI = new ArrayList<Integer>(I);
            for (int i = 0; i < I.size(); i++) {
                int j = 0;
                while (j < I.size()) {
                    if (beforeOrderedRPLList.get(i).P == RPLList.get(j).P)
                        break;
                    j++;
                }
                int x = 0;
                while (x < I.size()) {
                    if (I.get(x) == i)
                        break;
                    x++;
                }
                newI.set(x, j);
            }
            I = newI;
            ////////////Line 13-Line 15 END////////
            for (int i = 1; i < B_defined.size(); i++) {
                //B[i] is not defined
                if (!B_defined.get(i)) { //line 16
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
                        B.set(i, new TypeB(R1.charAt(cs), R2.charAt(cs), start + cs));
                        B_defined.set(i, true);
                        if (B_defined.get(i - 1) || i == 1) {
                            I_done.put(I.get(RPLList.get(i - 1).P), true);
                            A_done.set(i - 1, true);
                        }
                        if (i == RPLList.size() - 1 || B_defined.get(i + 1)) {
                            I_done.put(I.get(RPLList.get(i).P), true);
                            A_done.set(i, true);
                        }
                    }
                }
            }
            start += range;
        }

        Object[] LB = new Object[2];
        List<int[]> newL = new LinkedList<int[]>();
        for (RPL rpl : RPLList)
            newL.add(rpl.L);
        LB[0] = newL;
        LB[1] = B;
        return LB;
    }

    public TreeNode buildSubTree(List<int[]> L, List<TypeB> B) {
        TreeNode root = new SlavesWorks.TreeNode();
        TreeNode u_ = new SlavesWorks.TreeNode();
        root.parent = null;
        root.leftChild = u_;
        u_.parent = root;
        //L-B只有一个元素
        int[] L0 = L.get(0);
        String e_ = S.get(L0[0]).substring(L0[1]);
        u_.data = e_;
        u_.index = L0;
        Stack<TreeNode> stack = new Stack<TreeNode>();
        stack.push(u_);
        Map<TreeNode, Integer> v1Length = new HashMap<TreeNode, Integer>();//从根节点到某一结点走过的字符串的长度
        v1Length.put(root, 0);
        int depth = e_.length();
        for (int i = 1; i < B.size(); i++) {
            TypeB typeB = B.get(i);
            int offset = typeB.common;
            TreeNode v1, v2, u;
            do {
                TreeNode se = stack.pop();
                v1 = se.parent;
                v2 = se;
                depth -= se.data.length();
            } while (depth > offset);
            if (depth == offset) {
                u = v1.leftChild;
            } else {
                String se = v2.data;
                //寻找根节点到v1经历了几个字符
                int before = v1Length.get(v1);
                int end = offset - before;//跳过根节点到v1这么长的字符
                if (end > se.length()) {
                    end = se.length();
                }
                v2.data = se.substring(0, end);
                TreeNode vt = new TreeNode(se.substring(end), v2.index);
                if (v2.leftChild != null) {
                    vt.leftChild = v2.leftChild;
                    //修改v2孩子的父母为vt
                    v2.leftChild.parent = vt;
                    TreeNode sibling = v2.leftChild;
                    while (sibling.rightSibling != null) {
                        sibling.rightSibling.parent = vt;
                        sibling = sibling.rightSibling;
                    }
                }
                v2.leftChild = vt;
                vt.parent = v2;
                v2.index = null;
                //根节点到拆分形成的节点经过的字符串就是offset
                v1Length.put(v2, offset);
                u = vt;
                stack.push(v2);
                depth += v2.data.length();
            }
            u_ = new TreeNode();
            TreeNode next = u;
            while (next.rightSibling != null)
                next = next.rightSibling;
            next.rightSibling = u_;
            u_.parent = next.parent;
            //标记(u,u'),遇到终结符则停止
            //L.get(i)+offset到末尾，存在$0则不要了
            int[] Li = L.get(i);
            String sLi = S.get(Li[0]);//第Li[0]个字符串
            int end = Li[1] + offset;
            //计算end越界后，则只保留终结符
            if (end >= sLi.length())
                end = sLi.length() - 1;
            u_.data = sLi.substring(end);
            u_.index = Li;
            stack.push(u_);
            depth += u_.data.length();
        }
        return root;
    }

    //拆法1，对被拆节点位置不变，新建节点，把被拆节点信息转移到新节点上，新节点作为被拆节点的左孩子
    public void splitSubTree(List<String> S, String p, TreeNode root) {
        TreeNode currNode = root.leftChild;
        int lastSplit = 0;
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < p.length(); i++) {
            if (p.charAt(i) == SPLITTER || p.charAt(i) == SPLITTER_INSERTION) {
                String data = currNode.data;
                //建立一个新节点，将被拆分节点的信息转移给新节点
                TreeNode newNode = new TreeNode(data.substring(i - lastSplit), currNode.index);
                currNode.index = null;
                path.append(data.substring(0, i - lastSplit));
                currNode.data = data.substring(0, i - lastSplit);
                if (currNode.leftChild != null) {//将原currNode的孩子节点作为新节点的孩子
                    newNode.leftChild = currNode.leftChild;
                    newNode.leftChild.parent = newNode;
                }
                newNode.parent = currNode;
                currNode.leftChild = newNode;

                currNode = newNode;
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
                            TreeNode tmp = new TreeNode(line.charAt(line.length() - 1) + "", new int[]{j, start});
                            sibling.rightSibling = tmp;
                            tmp.parent = sibling.parent;
                            sibling = tmp;
                        }
                    }
                }
            }
        }
    }

    //拆法2，新建节点，并让新建节点作为被拆节点的父节点
    public void splitSubTree_V2(List<String> S, String p, TreeNode root) {
        TreeNode currNode = root.leftChild;
        int lastSplit = 0;
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < p.length(); i++) {
            if (p.charAt(i) == SPLITTER || p.charAt(i) == SPLITTER_INSERTION) {
                String data = currNode.data;
                TreeNode newNode = new TreeNode(data.substring(0, i - lastSplit), null);
                currNode.data = data.substring(i - lastSplit);
                currNode.parent.leftChild = newNode;
                newNode.parent = currNode.parent;
                newNode.leftChild = currNode;
                currNode.parent = newNode;
                //将currNode的兄弟转移给上层
                if (currNode.rightSibling != null) {
                    newNode.rightSibling = currNode.rightSibling;
//                    //调整转移后节点的父节点 不比要？？
                    //应该不要，curr的rightSib原来为curr的parent，现在在curr之上插入newNode，并把
                    //curr的rightSib给了newNode，则newNode.rightSib的parent不需要变
//                    TreeNode sibling = newNode.rightSibling;
//                    while (sibling != null) {
//                        sibling.parent = newNode.parent;
//                        sibling = sibling.rightSibling;
//                    }
                    currNode.rightSibling = null;
                }
                path.append(newNode.data);
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
                            TreeNode tmp = new TreeNode(line.charAt(line.length() - 1) + "", new int[]{j, start});
                            sibling.rightSibling = tmp;
                            tmp.parent = sibling.parent;
                            sibling = tmp;
                        }
                    }
                }
            }
        }
    }

    private Stack<TreeNode> path = new Stack<TreeNode>();
    private List<String> result = new ArrayList<String>();
    public void traverseTree(TreeNode root) {
        if (root == null) {
            if (path.isEmpty())
                return;
            TreeNode leaf = path.pop();
            if (leaf.index != null) {
                System.out.print(path.size() + " ");
                for (int i = 1; i < path.size(); i++)
                    System.out.print(path.get(i).data);
                System.out.println(leaf.data + " " + leaf.index[1]);
            }
        } else {
            path.push(root);
            traverseTree(root.leftChild);
            traverseTree(root.rightSibling);
        }
    }

    public void traverseTree(TreeNode root, Map<Character, String> terminatorFileName) {
        if (root == null) {
            if (path.isEmpty())
                return;
            TreeNode leaf = path.pop();
            if (leaf.index != null) {
                String line = String.format("%d %s:%d\n", path.size(), terminatorFileName.get(leaf.data.charAt(leaf.data.length() - 1)), leaf.index[1]);
                result.add(line);
//                System.out.println(path.size() + " " + terminatorFileName.get(leaf.data.charAt(leaf.data.length() - 1)) + ":" + leaf.index[1]);
            }
        } else {
            path.push(root);
            traverseTree(root.leftChild, terminatorFileName);
            traverseTree(root.rightSibling, terminatorFileName);
        }
    }

    public void traverseTree(TreeNode root, Map<Character, String> terminatorFileName, BufferedWriter outputStream) {
        if (root == null) {
            if (path.isEmpty())
                return;
            TreeNode leaf = path.pop();
            if (leaf.index != null) {
                try {
                    outputStream.write(path.size() + " " + terminatorFileName.get(leaf.data.charAt(leaf.data.length() - 1)) + ":" + leaf.index[1]);
                    outputStream.newLine();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        } else {
            path.push(root);
            traverseTree(root.leftChild, terminatorFileName);
            traverseTree(root.rightSibling, terminatorFileName);
        }
    }
}
