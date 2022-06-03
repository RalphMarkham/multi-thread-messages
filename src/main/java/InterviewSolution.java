import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

public class InterviewSolution {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static void run(int threads, String path) {
        BiFunctionFixedOrderedExecutor<String> func = new BiFunctionFixedOrderedExecutor<>(threads);

        try {
            @SuppressWarnings("rawtypes") CompletableFuture[] futures = Files.lines(Paths.get(path))
                .parallel()
                // map each line into a CompletableFuture
                .map(l -> {
                    int firstIndx = l.indexOf('|');
                    int lastIndx = l.lastIndexOf('|');
                    CompletableFuture<Void> cf = null;
                    if(firstIndx > -1 && lastIndx > firstIndx) {
                        if (firstIndx == 0) {
                            // if message id is not present and processing time is present (|500|)
                            // then the producer needs to stop producing for 500ms.  This simulates
                            // a pause in incoming messages.

                            // ensure a valid completed future is provided
                            cf = CompletableFuture.completedFuture(null);
                            try {
                                Thread.sleep(Integer.parseInt(l.substring(1,lastIndx)));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                        } else {
                            // Because FixedOrderedExecutor / es, maintains the key Runnable Queue pairs,
                            // which are updated by es and one of its inner classes, just using
                            // CompletableFuture.runAsync() becomes a little more impractical in this stream.
                            cf = func.apply(() -> l.substring(0,firstIndx), new Consumer(l, firstIndx, lastIndx));
                        }
                    }
                    return cf;
                })
                .toArray(CompletableFuture[]::new);

            CompletableFuture.allOf(futures).join();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            func.shutdown();
        }
    }

    private static void usage() {
        System.out.println("InterviewSolution #ofConsumers Path");
        System.exit(1);
    }

    public static void main(String[] args) {
        if(args.length != 2) {
            usage();
        }

        String start = DATE_TIME_FORMATTER.format(LocalTime.now());
        int threadCount = Integer.parseInt(args[0]);
        String path = args[1];

        System.out.printf("%s - STARTING - Consumers: %d;\tFile: %s%n", start, threadCount, path);
        run(threadCount, path);

        String end = DATE_TIME_FORMATTER.format(LocalTime.now());
        System.out.printf("%s - END%n", end);
    }

    private static class Consumer implements Runnable {

        private final String line;
        private final Integer firstIndx;
        private final Integer lastIndx;

        public Consumer(String line, Integer firstIndx, Integer lastIndx) {
            this.line = line;
            this.firstIndx = firstIndx;
            this.lastIndx = lastIndx;
        }

        @Override
        public void run() {
            String start = DATE_TIME_FORMATTER.format(LocalTime.now());
            int sleepDuration = Integer.parseInt(line.substring(firstIndx+1,lastIndx));
            long threadId = Thread.currentThread().getId();

            try {
                Thread.sleep(sleepDuration);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String end = DATE_TIME_FORMATTER.format(LocalTime.now());
            System.out.println(line+";  \tThread: "+threadId+";\tStart: "+start+";\tEnd: "+end);
        }
    }
}
