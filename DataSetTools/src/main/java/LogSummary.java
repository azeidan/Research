import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class LogSummary {

    private static String strNumericPattern = "\\d+(\\.\\d+)?";

    class PartitionCounts {
        int roundNum;
        int partitionNum;
        long positive = 1;
        long negative = 1;

        public PartitionCounts(int roundNum, int partitionNum) {

            this.partitionNum = partitionNum < 0 ? -partitionNum - 1 : partitionNum;
            this.roundNum = roundNum;

            if (partitionNum < 0)
                ++negative;
            else
                ++positive;
        }

        public int getRoundNum() {
            return roundNum;
        }

        public int getPartitionNum() {
            return partitionNum;
        }

        public String getGroupKey() {

            return String.format("%d-%d", roundNum, partitionNum);
        }
    }

    public LogSummary(Path pathIn, Path pathOut, long numLines, int numRounds) {

        Pattern pattern = Pattern.compile("\\t");

        double dNumLines = numLines;

        try (BufferedWriter bw = Files.newBufferedWriter(pathOut)) {

            AtomicInteger atomicInteger = new AtomicInteger(0);
            AtomicInteger atomicIntegerLineCount = new AtomicInteger(0);

            Files.lines(pathIn)
                    .flatMap(line -> {

                                System.out.printf("\r%.2f%%", (atomicIntegerLineCount.incrementAndGet() / dNumLines * 100));

                                atomicInteger.set(0);

                                return pattern
                                        .splitAsStream(line)
                                        .skip(1)
                                        .map(Integer::parseInt)
                                        .map(partNum -> new PartitionCounts(atomicInteger.incrementAndGet(), partNum));
                            }
                    )
                    .collect(Collectors.groupingBy(PartitionCounts::getPartitionNum, TreeMap::new,
                            Collectors.groupingBy(PartitionCounts::getRoundNum, Collectors.counting())))
                    .forEach((partNum, mapRoundCount) -> {

                        try {
                            bw.write(partNum + "\t" +
                                    IntStream.rangeClosed(0, numRounds)
                                            .mapToObj(roundNum -> {

                                                Long roundCount = mapRoundCount.getOrDefault(roundNum, 0L);

                                                return roundCount + "\t";
                                            }).collect(Collectors.joining(" "))

                                    + "\n");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        try {

            Long numLines = Long.parseLong(args[0]);
            Integer numRounds = Integer.parseInt(args[1]);
            Path pathIn = Paths.get(args[2]);
            Path pathOut = Paths.get(args[3]);

            if (numLines <= 0 || numRounds <= 0 || !Files.exists(pathIn))
                throw new Exception();

            new LogSummary(pathIn, pathOut, numLines, numRounds);
        } catch (Exception ex) {

            if (args != null) {

                if (args.length > 0)
                    if (args[0] != null && args[0].matches(strNumericPattern))
                        System.out.println("\u2714 Number of lines: " + args[0]);
                    else
                        System.err.println("\u2716 Invalid number of lines: " + args[0]);
                else
                    System.err.println("\u2716 Missing number of lines");

                if (args.length > 1)
                    if (args[1] != null && args[1].matches(strNumericPattern))
                        System.out.println("\u2714 Number of rounds: " + args[1]);
                    else
                        System.err.println("\u2716 Invalid number of rounds: " + args[1]);
                else
                    System.err.println("\u2716 Missing number of rounds");

                if (args.length > 2)
                    if (args[2] != null && Files.exists(Paths.get(args[2])))
                        System.out.println("\u2714 Input file: " + args[2]);
                    else
                        System.err.println("\u2716 Invalid input file: " + Paths.get(args[2]).toAbsolutePath().toString());
                else
                    System.err.println("\u2716 Missing input file");

                if (args.length > 3)
                    if (args[3] != null && Files.exists(Paths.get(args[3])))
                        System.out.println("\u2714 Output file: " + args[3]);
                    else
                        System.err.println("\u2716 Invalid output file: " + Paths.get(args[3]).toAbsolutePath().toString());
                else
                    System.err.println("\u2716 Missing output file");
            }

            System.err.println("Usage: java LogSummary <input log file> <output file> <line count> <round count>");
        }
    }
}
