package org.apache.samza.context;

import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class TestJobContextMetadata {


    private JobContextMetadata jobContextMetadata;


    @Before
    public void setup() {
        jobContextMetadata = new JobContextMetadata(null, null);
    }


    /**
     * Given a registered object, fetchObject should get it. If an object is not registered at a key, then fetchObject
     * should return null.
     */
    @Test
    public void testRegisterAndFetchObject() {
        String value = "hello world";
        jobContextMetadata.registerObject("key", value);
        assertEquals(value, jobContextMetadata.fetchObject("key"));
        assertNull(jobContextMetadata.fetchObject("not a key"));
    }

}