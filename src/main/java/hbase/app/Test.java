package hbase.app;

/**
 * @program: demo
 * @GitHub: https://github.com/ABHSY
 * @author: ABHSY.Klein
 * @create: 2018-09-10 22:58
 * -server -Xmx10m -Xms10m -XX:+DoEscapeAnalysis -XX:+PrintGC
 **/
public class Test {
//   static byte[] b;  1250
    public static void alloc() {
    /**
     * [GC 2624K->380K(9856K), 0.0010952 secs]
     * [GC 3004K->396K(9856K), 0.0163505 secs]
     * 257
     */
       byte[] b = new byte[2];
        b[0] = 1;
    }

    public static void main(String[] args) {
        long b = System.currentTimeMillis();
        for (int i = 0; i < 100000000; i++) {
            alloc();
        }
        long e = System.currentTimeMillis();
        System.out.println(e - b);
    }

}
