/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package rocks.nifi.examples.processors;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Set;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author phillip
 */
public class JsonProcessorTest {
    
    /**
     * Test of onTrigger method, of class JsonProcessor.
     */
    @org.junit.Test
    public void testOnTrigger() {
        InputStream content = new ByteArrayInputStream("{\"hello\":\"nifi rocks\"}".getBytes());
        
        TestRunner runner = TestRunners.newTestRunner(new JsonProcessor());
        runner.setProperty(JsonProcessor.JSON_PATH, "$.hello");
        runner.enqueue(content);
        runner.run();
        
        runner.assertQueueEmpty();
        
        // TODO review the generated test code and remove the default call to fail.
        // fail("The test case is a prototype.");
    }
    
}
