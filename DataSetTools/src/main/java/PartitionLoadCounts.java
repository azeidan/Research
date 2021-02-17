import com.google.common.util.concurrent.AtomicDouble;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PartitionLoadCounts {

    private static final String USAGE = "Usage: java " + PartitionLoadCounts.class.getName() + " inputFile lineCount numberOfPartitions numberOfRounds";

    public static void main(String[] args) throws Exception {

        if (args.length == 4) {

            String inputFile = args[0];
            long lineCount = Long.parseLong(args[1]);
            int numberOfPartitions = Integer.parseInt(args[2]);
            int numberOfRounds = Integer.parseInt(args[3]);

            int[][] arr = new int[numberOfPartitions][numberOfRounds];

            AtomicDouble counter = new AtomicDouble();

            Files.newBufferedReader(Paths.get(inputFile))
                    .lines()
                    .forEach(line -> {

                        counter.addAndGet(1);

                        if ((counter.intValue()) % 1000 == 0)
                            System.out.printf("\r%,.4f%%", (counter.doubleValue() / lineCount) * 100);

                        if (line.startsWith(">>>")) {

                            String[] parts = line.split("\t");

                            IntStream.rangeClosed(1, numberOfRounds).forEach(roundNum -> {
                                int partId = Integer.parseInt(parts[roundNum]);
                                if (partId < 0) partId = -partId - 1;

                                arr[partId][roundNum - 1]++;
                            });
                        }
                    });
            System.out.println();
            Arrays.stream(arr).forEach(row -> {
                Arrays.stream(row).forEach(col -> {
                    System.out.print(col + "\t");
                });
                System.out.println();
            });
        } else {
            System.err.println(USAGE);
            System.err.println("GOt: " + Arrays.stream(args).collect(Collectors.joining(" ")));
        }
    }
}
