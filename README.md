Multiple Thread Processing
--------------------------
This task simulates receiving messages over a network stream and processing them efficiently.
The messages are received one at a time (serialized) so we are using a single producer,
but to possibly increase throughput, we want to use multiple processors.

We are asking you to use one thread to read the file one line at a time and a configurable
number of threads to handle processing messages.

A sample file (multi_thread_messages.txt) is included that contains
one message per line where values are delimited by '|'.

The first value in each line is the message ID.
- message ID is of type String
- message IDs can and will repeat
- number of unique IDs is unknown
- format of message IDs is unknown (ie. don't assume one character from A-Z)

The second value is how long it takes to process the message (in milliseconds) and the third value is the
payload.
none of the values will contain the delimiter so you can assume there will at most be two '|' characters
per line.

if message id is not present and processing time is present (|500|) then the producer needs to stop
producing for 500ms.  This simulates a pause in incoming messages.

The second value (the number of ms processing will take) can not be used for anything other than sleeping.  In a real world scenario,
processing time or delays between messages will not be know and processing will take whatever time it takes.
This value must not be used in any part of the algorithm.

All messages with the same id must be processed in the order they appear in the file.

So for example, if these are the messages in the file:

A|1000|Monday
B|1000|Wednesday
D|3000|Friday
A|50|Tuesday
B|100|Thursday
|10000|
D|100|Saturday

then 'Monday', 'Wednesday', 'Friday' can be processed at the same time independently of each other,
and 'Tuesday', 'Thursday', 'Saturday' can be processed at the same time independently of each other,
but 'Tuesday' must be processed after 'Monday', and 'Thursday' after 'Wednesday' and 'Saturday' after 'Friday'
The producer will pause for 10 seconds before sending 'Saturday' for processing.

Each line provides the value for how long the consumer should sleep to simulate the message processing time.

Write a program (Java or an Object Oriented language of your choice) without using external third party libraries.
You cannot sort the file, or read through the entire file before sending items to process.
Must treat each line in the file as if you do not know the next line.

Include a Readme.txt describing the algorithm in detail so that it can be understood by developers and technical business analysts.
As part of the source code, please use comments to explain the design choices.

The program should accept consumer count and source file location as a parameters.  If written in Java:
- java InterviewSolution 5 multi_thread_messages.txt

Output of the program should be the <ORIGINAL_MESSAGE>;  Thread: <THREAD_ID>;  Start: <HH:mm:ss.SSS>;  End: <HH:mm:ss.SSS>


(The below output sample is for formatting only. The accuracy of thread ids, order, start and end times may not be correct)
Output Sample:

09:12:10.000 - STARTING - Consumers: 5;  File: .../multi_thread_messages.txt

A|1000|Monday;    Thread: XX;  Start: 09:12:10.000;  End: 09:12:11.000
B|1000|Tuesday;   Thread: XX;  Start: 09:12:10.000;  End: 09:12:11.000
A|50|Wednesday;   Thread: XX;  Start: 09:12:11.000;  End: 09:12:11.050
B|100|Thursday;   Thread: XX;  Start: 09:12:11.000;  End: 09:12:11.100
D|3000|Friday;    Thread: XX;  Start: 09:12:10.000;  End: 09:12:13.000
D|100|Saturday;   Thread: XX;  Start: 09:12:23.000;  End: 09:12:23.100

09:12:23.100 - END

# Solution
This solution uses `Files.lines(Paths.get(path))` to create a stream of lines that are individually wrapped in a 
`Runnable` processor class `Consumer` and passed to an `ExecutionSevice` like instance of 
`FixedOrderedExecutor`, along with the message id, to be executed.  

The executor class `FixedOrderedExecutor` has two instance fields, a delegate `ExecutorService` and 
a `Map` made up of string key, and a `Queue` of `Runnable`.  The execute method takes the message id key, 
and attempts to retrieve the corresponding `Queue`, if the queue exists, the `Consumer` is added to it, otherwise
a new instance of the inner class `TaskQueue`, a `Runnable` `LinkedList` is created and the `Consumer` is added, 
before submitting the new instance of `TaskQueue` to be executed on the delegate `ExecutorService`.

Fixed ugly hack, using `CompletableFuture<Void>`.