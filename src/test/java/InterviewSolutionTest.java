import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;


class InterviewSolutionTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;


    @BeforeEach
    void setUp() {
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    void tearDown() {
        System.setOut(originalOut);
    }

    @Test
    void main() {
        InterviewSolution.main(new String[]{"5","multi_thread_messages.txt"});

        long count = outContent.toString()
                .lines()
                .filter(s -> s.contains("Thread: "))
                .sorted()
                .map(s -> {
                    int indx = s.indexOf("Thread: ") + "Thread: ".length();
                    return s.charAt(0)+s.substring(indx,indx+2);
                })
                .distinct()
                .peek(System.err::println)
                .count();

        // Expecting:
        // 1) A##
        // 2) B##
        // 3) C##
        // 4) D##
        // 5) E##
        // 6) Z##

        Assertions.assertEquals(6, count);
    }
}