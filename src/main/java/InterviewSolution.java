import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class InterviewSolution {
    private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private static void run(int threads, String path) {
        FixedOrderedExecutor<String> es = new FixedOrderedExecutor<>(threads);

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

    /**
     * All messages with the same id must be processed in the order they appear in the file.
     *
     * A simple delegate ExecutorService, extended to support the execution
     * of key Runnable pairs, ensuring sequential execution based on key.
     * Key based sequential execution is managed by maintaining a map of key
     * Runnable Queue pairs. Upon completion the Runnable Queue inner class
     * removes itself from the map.
     *
     * @param <K> the key type
     */
    private static class FixedOrderedExecutor<K> implements ExecutorService {

        /**
         *  A simpler version of AsyncRun from CompletableFuture, used to
         *  couple the Runnable and CompletableFuture inorder to provide a
         *  more elegant solution for maintaining a connection to the Runnable
         *  after it has been submitted.
         */
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

        /**
         * Wrapper ensuring the sequential execution of key Runnable pairs.
         *
         * Under normal circumstances an I would classify this kind inheritance
         * as an anti-pattern, much better served by composition, as inheritance
         * does not provide any encapsulation to its super classes, but as a
         * private inner class, it provides both a simple and effective data structure
         * encapsulated in the FixedOrderedExecutor class.
         */
        private class TaskQueue extends LinkedList<Runnable> implements Runnable {
            private final K key;

            public TaskQueue(K key, Runnable runnable) {
                super();

                Objects.requireNonNull(runnable, "Everybody knows a Runnable can not be null, please rethink your options");

                this.key = key;
                this.push(runnable);
            }

            @Override
            public void run() {
                // Under normal circumstances, the potential for throwing
                // a NullPointerException would exclude the use of do while.
                // But because we know our trusty constructor is verifying
                // a non-null runnable, do while is a safe bet.
                //
                // Do while we still have a Runnable to run
                do {
                    // Retrieve, remove, and run the head of the list
                    //noinspection ConstantConditions
                    this.poll().run();

                    // Optimize use of lock, as running this out side of the
                    // loop runs the chance that a runnable is added before
                    // this is remove from mappedTasks.
                    if (this.size() < 1) {
                        // Make sure we still have a Runnable to run,
                        // otherwise remove this list from mappedTasks
                        try {
                            lock.lock();
                            if (this.isEmpty()) {
                                mappedTasks.remove(key);
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                } while(!this.isEmpty());
            }
        }

        // A lock to synchronize when mappedTasks and it value objects
        // TaskQueue can be updated and / or created
        private final Lock lock = new ReentrantLock();

        // Works just fine with HashMap, use of ConcurrentHashMap purely performance
        // related.  Like the HashMap, ConcurrentHashMap requires the use of locking
        // as the multi-threaded behaviour is sort of like disengaging fire suppression
        // and alarm systems so you can do a marathon bake off, with out the worry of
        // being interrupted by the sprinklers or fire alarm going off.
        //
        // Validation of this can be found, by removing the locking and running the
        // corresponding unit test.
        private final Map<K, Queue<Runnable>> mappedTasks = new ConcurrentHashMap<>();
        private final ExecutorService delegate;

        public FixedOrderedExecutor(int threads) {
            delegate = Executors.newFixedThreadPool(threads);
        }

        public CompletableFuture<Void> submit(K key, Runnable runnable) {
            // Wrap the runnable with a CompletableFuture
            RunnableCompletableFuture run = new RunnableCompletableFuture(runnable, new CompletableFuture<>());
            try {
                lock.lock();
                // Search for an existing Queue of Runnable if found, add it to the Queue, otherwise
                // put the key and a new Runnable Queue, containing run into mappedTasks
                Queue<Runnable> queue = mappedTasks.get(key);
                if (queue != null) {
                    queue.add(run);
                } else {
                    TaskQueue tq = new TaskQueue(key, run);
                    mappedTasks.put(key, tq);
                    delegate.execute(tq);
                }
            } finally {
                lock.unlock();
            }

            return run.cf;
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
