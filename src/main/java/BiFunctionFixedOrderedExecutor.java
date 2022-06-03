import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Supplier;


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
public final class BiFunctionFixedOrderedExecutor<K> implements BiFunction<Supplier<K>, Runnable, CompletableFuture<Void>> {

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
                } catch (Exception ex) {
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
        private transient final K key;

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
    private final Lock lock = new ReentrantLock();


    // Sequential execution of key based lists requires fail fast concurrent behaviour
    private final Map<K, Queue<Runnable>> mappedTasks = new HashMap<>();
    private final ExecutorService delegate;

    public BiFunctionFixedOrderedExecutor(int threads) {
        delegate = Executors.newFixedThreadPool(threads);
    }

    @Override
    public CompletableFuture<Void> apply(Supplier<K> kSupplier, Runnable consumer) {
        return submit(kSupplier.get(), consumer);
    }

    public void shutdown() {
        delegate.shutdown();
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
}