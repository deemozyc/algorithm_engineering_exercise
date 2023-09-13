import java.util.Arrays;
import java.util.Random;

public class Merge {
    // merge in mergeSort, Assuming distinct elements

    static long[] a, b, res;
    // merge a,b to res
    private static final Random RNG = new Random();

    private static boolean isSort(long[] arr) {
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] < arr[i - 1]) {
                return false;
            }
        }
        return true;
    }

    // TODO: add more test case
    private static Object[] twoSortedRandomArray(int size) {
        // Assuming distinct elements
        // firstly, a random array
        long[] arr = new long[size * 3];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = RNG.nextLong();
        }
        Arrays.sort(arr);
        // unique
        int index = 1;
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] != arr[i - 1] && index != i) {
                arr[index++] = arr[i];
            }
        }

        long[] a = new long[size], b = new long[size];
        int aIndex = 0, bIndex = 0;
        for (int i = 0; i < size * 2; i++) {
            if (bIndex >= size || RNG.nextLong() % 2 == 1) {
                a[aIndex++] = arr[i];
            } else {
                b[bIndex++] = arr[i];
            }
        }
        return new Object[] { a, b };

    }

    public static void mergeSeq(long[] left, long[] right, long[] res) {
        int i1 = 0, i2 = 0;
        for (int i = 0; i < res.length; i++) {
            if (i2 >= right.length || (i1 < left.length && left[i1] < right[i2])) {
                res[i] = left[i1++];
            } else {
                res[i] = right[i2++];
            }
        }
    }

    public static void mergePar(long[] left, long[] right, long[] res) {
        // assume left.length == right.length
        // TODO: remove this assume (edit some code)
        int size = left.length;
        int pnum = 1;
        a = left;
        b = right;
        Merge.res = new long[a.length + b.length];

        ParallelMerge tasks[] = new ParallelMerge[pnum];

        for (int i = 0; i < pnum; i++) {
            tasks[i] = new ParallelMerge(size / pnum * i, size / pnum * (i + 1));
            tasks[i].start();
        }
        try {
            for (int i = 0; i < pnum; i++) {
                tasks[i].join();
            }
        } catch (Exception e) {
            System.out.println("Error " + e);
        }
        res = Merge.res;
        // System.out.println("MergePar:" + Arrays.toString(res));

    }

    private static class ParallelMerge extends Thread {
        private int left;
        private int right;

        public ParallelMerge(int left, int right) {
            this.left = left;
            this.right = right;
        }

        public int binarySearch(long arr[], long x) {
            // assume x, arr[] are distinct
            // return a that: arr[a] < x < arr[a + 1]
            int l = 0, r = arr.length - 1;
            if (arr[l] > x) {
                return 0;
            }
            if (arr[r] < x) {
                return r + 1;
            }
            while (r - l > 1) {
                int m = l + (r - l) / 2;
                if (arr[m] < x)
                    l = m;
                else
                    r = m;
            }
            return r;
        }

        @Override
        public void run() {
            for (int i = left; i < right; i++) {
                if (i > b.length)
                    continue;
                int j = binarySearch(a, b[i]);
                // System.out.println("debug: i: " + i + " j: " + j + " b[i]: " + b[i]);
                res[i + j] = b[i];
            }
            for (int i = left; i < right; i++) {
                if (i > a.length)
                    continue;
                int j = binarySearch(b, a[i]);
                res[i + j] = a[i];
            }
        }
    }

    public static void init(int size) {
        long start = System.currentTimeMillis();
        Object[] temp = twoSortedRandomArray(size);
        a = (long[]) temp[0];
        b = (long[]) temp[1];
        res = new long[a.length + b.length];
        long end = System.currentTimeMillis();
        System.out.println("init data took: " + (end - start) + " Millis");

    }

    public static void runMergeSeq(int size) {
        init(size);

        long start = System.currentTimeMillis();
        mergeSeq(a, b, res);
        long end = System.currentTimeMillis();
        if (isSort(res) == false) {
            System.out.println("ERROR: Sort algorithm error!");
        }
        System.out.println("mergeSeq took: " + (end - start) + " Millis");
    }

    public static void runMergePar(int size, int pnum) {
        init(size);
        /*
         * //debug
         * a = new long[]{-54, -11, 21, 43};
         * b = new long[]{-26, -7, 4, 16};
         * System.out.println(Arrays.toString(a));
         * System.out.println(Arrays.toString(b));
         */

        long start = System.currentTimeMillis();
        mergePar(a, b, res);
        long end = System.currentTimeMillis();

        if (isSort(res) == false) {
            System.out.println("ERROR: Sort algorithm error!");
        }
        System.out.println("MergePar took: " + (end - start) + " Millis");
    }

    public static void main(String[] args) {
        int size = 1 << 20;
        int pnum = 16;
        runMergeSeq(size);
        runMergePar(size, pnum);
        runMergePar(size, pnum); // weird, just like Msort.java
        // System.out.println(Arrays.toString(res));

    }
}
