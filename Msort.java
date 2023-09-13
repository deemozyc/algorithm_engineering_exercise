import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.Random;

public class Msort {

    // static long[] arr;
    private static final Random RNG = new Random(42L);

    private static boolean isSort(long[] arr) {
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] < arr[i - 1]) {
                return false;
            }
        }
        return true;
    }

    // TODO: parallelMerge needs unique numbers
    private static long[] randomArray(int size) {
        long[] arr = new long[size];
        for (int i = 0; i < arr.length; i++)
            arr[i] = RNG.nextLong();
        return arr;
    }

    private static void mergeSortSeq(long[] arr) {
        if (arr.length < 2)
            return;
        long[] leftArr = Arrays.copyOfRange(arr, 0, arr.length / 2);
        long[] rightArr = Arrays.copyOfRange(arr, arr.length / 2, arr.length);
        mergeSortSeq(leftArr);
        mergeSortSeq(rightArr);

        Merge.mergeSeq(leftArr, rightArr, arr);
    }

    private static class ParallelMergeSort extends RecursiveAction {
        private long[] arr;

        public ParallelMergeSort(long[] arr) {
            this.arr = arr;
        }

        @Override
        protected void compute() {
            if (arr.length < 2)
                return;
            long[] leftArr = Arrays.copyOfRange(arr, 0, arr.length / 2);
            long[] rightArr = Arrays.copyOfRange(arr, arr.length / 2, arr.length);
            ParallelMergeSort leftTask = new ParallelMergeSort(leftArr);
            ParallelMergeSort rightTask = new ParallelMergeSort(rightArr);
            invokeAll(leftTask, rightTask);
            leftTask.join();
            rightTask.join();
            Merge.mergeSeq(leftArr, rightArr, arr);

        }
    }

    private static class FullParallelMergeSort extends RecursiveAction {
        private long[] arr;

        public FullParallelMergeSort(long[] arr) {
            this.arr = arr;
        }

        @Override
        protected void compute() {
            if (arr.length < 2)
                return;
            long[] leftArr = Arrays.copyOfRange(arr, 0, arr.length / 2);
            long[] rightArr = Arrays.copyOfRange(arr, arr.length / 2, arr.length);
            FullParallelMergeSort leftTask = new FullParallelMergeSort(leftArr);
            FullParallelMergeSort rightTask = new FullParallelMergeSort(rightArr);
            invokeAll(leftTask, rightTask);
            leftTask.join();
            rightTask.join();
            Merge.mergeSeq(leftArr, rightArr, arr);
            // FIXME: following calling has bug somehow. But in Merge.java it is OK.
            // Merge.mergePar(leftArr, rightArr, arr);

        }
    }

    public static void runMergeSortSeq(int size) {
        long[] arr = randomArray(size);
        // System.out.println(Arrays.toString(arr));
        long start = System.currentTimeMillis();
        mergeSortSeq(arr);
        long endt = System.currentTimeMillis();
        // System.out.println(Arrays.toString(arr));
        if (isSort(arr) == false) {
            System.out.println("ERROR: Sort algorithm error!");
        }
        System.out.println("runMergeSortSeq took " + (endt - start) + " Millis");
    }

    public static void runParallelMergeSort(int size) {
        long[] arr = randomArray(size);

        long start = System.currentTimeMillis();
        ForkJoinPool pool = new ForkJoinPool();
        ParallelMergeSort topTask = new ParallelMergeSort(arr);
        pool.invoke(topTask);
        topTask.join();
        long endt = System.currentTimeMillis();

        if (isSort(arr) == false) {
            System.out.println("ERROR: Sort algorithm error!");
        }
        System.out.println("runParallelMergeSort took: " + (endt - start) + " Millis");
    }

    public static void runFullParallelMergeSort(int size) {
        long[] arr = randomArray(size);

        long start = System.currentTimeMillis();
        ForkJoinPool pool = new ForkJoinPool();
        FullParallelMergeSort topTask = new FullParallelMergeSort(arr);
        pool.invoke(topTask);
        topTask.join();
        long endt = System.currentTimeMillis();

        if (isSort(arr) == false) {
            System.out.println("ERROR: Sort algorithm error!");
        }
        System.out.println("runFullParallelMergeSort took: " + (endt - start) + " Millis");
    }

    public static void main(String[] args) {
        int size = 1 << 20;
        runMergeSortSeq(size);
        runParallelMergeSort(size); // weird, the first one parallel is always slow
        runParallelMergeSort(size); // and the same run twice is much faster
        runFullParallelMergeSort(size);
        // System.out.println(Arrays.toString(arr));

    }
}
