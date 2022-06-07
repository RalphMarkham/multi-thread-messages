import ca.ralphsplace.concurrency.SeqMultiValueMapScheduler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

public class InterviewSolution {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static void run(int threads, String path) {
        var scheduler = new SeqMultiValueMapScheduler<>(threads);

        try {
            //noinspection resource
            var futures = Files.lines(Paths.get(path)) // NOSONAR: maybe sonar and Oracle can sit down and talk this out.
                .parallel() // nothing breaks thread safety quite like threads spawning threads
                // map each line into a CompletableFuture
                .map(l -> {
                    var firstIndx = l.indexOf('|');
                    var lastIndx = l.lastIndexOf('|');

                    CompletableFuture<String> cf = null;
                    if(firstIndx > -1 && lastIndx > firstIndx) {
                        if (firstIndx == 0) {
                            // Simulate a pause in message stream
                            //
                            // if message id is not present and processing time is present (|500|)
                            // then the producer needs to stop producing for 500ms.

                            // ensure a valid completed future is provided
                            // would have preferred new Void() over null
                            cf = CompletableFuture.completedFuture(null);
                            try {
                                Thread.sleep(Integer.parseInt(l.substring(1,lastIndx)));//NOSONAR
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            // Process Line.
                            cf = scheduler.submit(l.substring(0,firstIndx), new LineProcessor(l, firstIndx, lastIndx));
                        }
                    }
                    return cf;
                })
                .toArray(CompletableFuture[]::new);

            CompletableFuture.allOf(futures).join();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            scheduler.shutdown();
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

    private static class LineProcessor implements Callable<String> {

        private final String line;
        private final Integer firstIndx;
        private final Integer lastIndx;

        public LineProcessor(String line, Integer firstIndx, Integer lastIndx) {
            this.line = line;
            this.firstIndx = firstIndx;
            this.lastIndx = lastIndx;
        }

        @Override
        public String call() {
            String start = DATE_TIME_FORMATTER.format(LocalTime.now());
            int sleepDuration = Integer.parseInt(line.substring(firstIndx+1,lastIndx));
            long threadId = Thread.currentThread().getId();

            try {
                Thread.sleep(sleepDuration);//NOSONAR
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String end = DATE_TIME_FORMATTER.format(LocalTime.now());
            System.out.println(line+";  \tThread: "+threadId+";\tStart: "+start+";\tEnd: "+end);

            return line;
        }
    }
}
