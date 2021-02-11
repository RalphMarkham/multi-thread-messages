# multi-thread-messages

This solution uses `Files.lines(Paths.get(path))` to create a stream of lines that are individually wrapped in a 
`Runnable` processor class `Consumer` and passed to an `ExecutionSevice` like instance of 
`FixedOrderedExecutor`, along with the message id, to be executed.  

The executor class `FixedOrderedExecutor` has two instance fields, a delegate `ExecutorService` and 
a `Map` made up of string key, and a `Queue` of `Runnable`.  The execute method takes the message id key, 
and attempts to retrieve the corresponding `Queue`, if the queue exists, the `Consumer` is added to it, otherwise
a new instance of the inner class `TaskQueue`, a `Runnable` `LinkedList` is created and the `Consumer` is added, 
before submitting the new instance of `TaskQueue` to be executed on the delegate `ExecutorService`.

This implementation is not something I would suggest maintaining, or building on. A production level solution 
would require a properly extending the `ExecutorService` interface in a way that properly supports past, current, and 
future concurrency api's. Extended `ExecutorService` interface and builder pattern for creating its instance.




