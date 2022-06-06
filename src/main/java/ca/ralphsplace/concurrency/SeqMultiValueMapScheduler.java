package ca.ralphsplace.concurrency;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public final class SeqMultiValueMapScheduler<K> {

    // A lock to synchronize when mappedTasks and it value-objects
    private final Lock lock = new ReentrantLock();


    // Sequential execution of key based lists requires fail fast concurrent behaviour
    private final Map<K, Queue<Runnable>> mappedTasks = new HashMap<>();
    private final ExecutorService exeSrv;

    public SeqMultiValueMapScheduler(int threads) {
        exeSrv = Executors.newFixedThreadPool(threads);
    }

    public void shutdown() {
        exeSrv.shutdown();
    }

    public <V> CompletableFuture<V> submit(K key, Callable<V> callable) {
        // Wrap the runnable with a CompletableFuture
        var run = new AsyncCallable<>(callable, new CompletableFuture<>());
        try {
            lock.lock();
            // Search for an existing Queue of Runnable if found, add it to the Queue, otherwise
            // put the key and a new Runnable Queue, containing run into mappedTasks
            var queue = mappedTasks.get(key);
            if (queue != null) {
                queue.add(run);
            } else {
                var tq = new TaskQueue(key, run);
                mappedTasks.put(key, tq);
                exeSrv.execute(tq);
            }
        } finally {
            lock.unlock();
        }

        return run.cf;
    }

    /**
     *  A simpler version of AsyncRun from CompletableFuture, used to
     *  couple the Runnable and CompletableFuture inorder to provide a
     *  more elegant solution for maintaining a connection to the Runnable
     *  after it has been submitted.
     */

    private static class AsyncCallable<V> implements Runnable {
        private final Callable<V> r;
        private final CompletableFuture<V> cf;


        public AsyncCallable(Callable<V> r, CompletableFuture<V> cf) {
            this.r = r;
            this.cf = cf;
        }

        @Override
        public void run() {
            if (r != null && cf != null) {
                try {
                    cf.complete(r.call());
                } catch (Exception ex) {
                    cf.obtrudeException(ex);
                }
            }
        }
    }

    /**
     * Wrapper ensuring the sequential execution of key Runnable pairs.
     *
     * Under normal circumstances I would classify this kind inheritance
     * as an anti pattern, much better served by composition, as inheritance
     * does not provide any encapsulation to its super classes, but as a
     * private inner class, it provides both a simple and effective data structure
     * encapsulated in the ca.ralphsplace.concurrency.SeqMultiValueMapScheduler class.
     *
     */
    private class TaskQueue extends LinkedList<Runnable> implements Runnable { //NOSONAR
        private final transient K key;

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

                // Optimize use of lock, as running this outside of the
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof SeqMultiValueMapScheduler.TaskQueue)) return false;
            if (!super.equals(o)) return false;
            SeqMultiValueMapScheduler<?>.TaskQueue taskQueue = (SeqMultiValueMapScheduler<?>.TaskQueue) o;
            return key.equals(taskQueue.key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), key);
        }
    }
}