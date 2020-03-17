package cn.flink.test;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class StreamingFileSinkTest extends TestLogger {

    @ClassRule
    public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

    @Test
    public void testFolder() throws IOException {
        System.out.println(TEMP_FOLDER.newFolder().toURI());

    }

    @Test
    public void testClosingWithoutInput() throws Exception {
        final File outDir = TEMP_FOLDER.newFolder();
        try (
            OneInputStreamOperatorTestHarness<Tuple2<String,Integer>, Object> testHarness =
                    TestUtils.createRescalingTestSink(outDir,1,0,100L,124L);
        ) {
            testHarness.setup();
            testHarness.open();
        }
    }

    @Test
    public void testClosingWithoutInitializingStateShouldNotFail() throws Exception {
        final File outDir = TEMP_FOLDER.newFolder();

        try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness =
                     TestUtils.createRescalingTestSink(outDir, 1, 0, 100L, 124L)) {
            testHarness.setup();
        }
    }

    @Test
    public void testTruncateAfterRecoveryAndOverwrite() throws Exception {
        //final File outDir = TEMP_FOLDER.newFolder();
        final File outDir = new File("D:\\streamFile");
        OperatorSubtaskState snapshot;

        // we set the max bucket size to small so that we can know when it rolls
        try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness = TestUtils.createRescalingTestSink(
                outDir, 1, 0, 100L, 10L)) {

            testHarness.setup();
            testHarness.open();

            // this creates a new bucket "test1" and part-0-0
            testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 1), 1L));
            //TestUtils.checkLocalFs(outDir, 1, 0);


            // we take a checkpoint so that we keep the in-progress file offset.
            snapshot = testHarness.snapshot(1L, 1L);

            // these will close part-0-0 and open part-0-1
            testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 2), 2L));
            testHarness.processElement(new StreamRecord<>(Tuple2.of("test1", 3), 3L));

            //TestUtils.checkLocalFs(outDir, 2, 0);

            System.out.println("========================================================");
            Map<File, String> contents = TestUtils.getFileContentByPath(outDir);
            System.out.println(contents);

            int fileCounter = 0;
            for (Map.Entry<File, String> fileContents: contents.entrySet()) {
                if (fileContents.getKey().getName().contains(".part-0-0.inprogress")) {
                    fileCounter++;
                    Assert.assertEquals("test1@1\ntest1@2\n", fileContents.getValue());
                } else if (fileContents.getKey().getName().contains(".part-0-1.inprogress")) {
                    fileCounter++;
                    Assert.assertEquals("test1@3\n", fileContents.getValue());
                }
            }

            Assert.assertEquals(2L, fileCounter);
        }

        try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Object> testHarness = TestUtils.createRescalingTestSink(
                outDir, 1, 0, 100L, 10L)) {
            testHarness.setup();
            testHarness.initializeState(snapshot);
            testHarness.open();

            // the in-progress is the not cleaned up one and the pending is truncated and finalized
            TestUtils.checkLocalFs(outDir, 2, 0);
        }
    }
}
