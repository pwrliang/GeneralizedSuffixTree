package GST;

/**
 * Created by Liang on 2017/4/4.
 */
public class Test1 {
    public static void main(String[] args) {
        String s = "AAAAAAA";
        int[] A = new int[s.length()];
        int lastId = 0;
        boolean stateEqual = false;
        for (int i = 0; i < s.length() - 1; i++) {
            //初态相邻两个不相等
            //相等-》不相等
            if (!stateEqual && s.charAt(i) == s.charAt(i + 1)) {
                stateEqual = true;
                lastId++;
                A[i] = lastId;
                A[i + 1] = lastId;
            } else if (stateEqual && s.charAt(i) != s.charAt(i + 1)) {
                stateEqual = false;
                A[i] = lastId;
                lastId++;
                //处理最后一个元素
                if (i + 1 == A.length - 1)
                    A[i + 1] = lastId;
            } else {
                A[i] = lastId;
                A[i + 1] = lastId;
            }

        }
        System.out.println(A);
    }
}
