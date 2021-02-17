import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class InterviewSolution {
    private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static void run(int threads, String path) {
        FixedOrderedExecutor<String> es = new FixedOrderedExecutor<>(threads);

        try {
            @SuppressWarnings("rawtypes")
            CompletableFuture[] futures = Files.lines(Paths.get(path))
                    .map(l -> {
                        int firstIndx = l.indexOf('|');
                        int lastIndx = l.lastIndexOf('|');
                        CompletableFuture<Void> cf = CompletableFuture.completedFuture(null);
                        if(firstIndx > -1 && lastIndx > firstIndx) {
                            if (firstIndx == 0) {
                                try {
                                    Thread.sleep(Integer.parseInt(l.substring(1,lastIndx)));
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                            } else {
                                cf = es.submit(l.substring(0,firstIndx), new Consumer(l, firstIndx, lastIndx));
                            }
                        }
                        return cf;
                    })
                    .toArray(CompletableFuture[]::new);

            CompletableFuture.allOf(futures).join();

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            es.shutdown();
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

        System.out.printf("%s - STARTING - Consumers: %d;\tFile: %s\n", start, threadCount, path);
        run(threadCount, path);

        String end = DATE_TIME_FORMATTER.format(LocalTime.now());
        System.out.printf("%s - END\n", end);
    }

    static class Consumer implements Runnable {

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

    static class FixedOrderedExecutor<K> implements ExecutorService {

        private final ExecutorService delegate;
        private final Map<K, Queue<Runnable>> mappedTasks = new HashMap<>();

        public FixedOrderedExecutor(int threads) {
            delegate = Executors.newFixedThreadPool(threads);
        }

        public CompletableFuture<Void> submit(K key, Runnable runnable) {
            RunnableCompletableFuture run = new RunnableCompletableFuture(runnable, new CompletableFuture<>());
            synchronized (mappedTasks) {
                Queue<Runnable> queue = mappedTasks.get(key);
                if (queue != null) {
                    queue.add(run);
                } else {
                    TaskQueue tq = new TaskQueue(key, run);
                    mappedTasks.put(key, tq);
                    delegate.execute(tq);
                }
            }
            return run.cf;
        }

        private static class RunnableCompletableFuture implements Runnable {
            private final Runnable r;
            private final CompletableFuture<Void> cf;

            public RunnableCompletableFuture(Runnable r, CompletableFuture<Void> cf) {
                this.r = r;
                this.cf = cf;
            }

            @Override
            public void run() {
                if (r != null && cf != null) {
                    try {
                        r.run();
                        cf.complete(null);
                    } catch (Throwable ex) {
                        cf.obtrudeException(ex);
                    }
                }
            }
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        class TaskQueue extends LinkedList<Runnable> implements Runnable {
            private final K key;
            public TaskQueue(K key, Runnable runnable) {
                super();
                this.key = key;
                this.add(runnable);
            }

            @Override
            public void run() {
                while(!this.isEmpty()) {
                    this.poll().run();
                    synchronized (this) {
                        if (this.isEmpty()) {
                            synchronized (mappedTasks) {
                                mappedTasks.remove(key);
                            }
                        }
                    }
                }
            }
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return delegate.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return delegate.submit(task, result);
        }

        @Override
        public Future<?> submit(Runnable task) {
            return delegate.submit(task);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
            return delegate.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return delegate.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return delegate.invokeAny(tasks, timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            delegate.execute(command);
        }
    }
}
