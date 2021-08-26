package com.gasa.jinggao.lambda.demo;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.gsas.jinggao.lambda.demo.changeServerEntryUrl;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
public class changeServerEntryUrlTest {

    private SNSEvent input;

    @Before
    public void createInput() throws IOException {
        // TODO: set up your sample input object here.
        input = TestUtils.parse("/sns-event.json", SNSEvent.class);
    }

    private Context createContext() {
        TestContext ctx = new TestContext();

        // TODO: customize your context here if needed.
        ctx.setFunctionName("Your Function Name");

        return ctx;
    }

    @Test
    public void testchangeServerEntryUrl() {
        changeServerEntryUrl handler = new changeServerEntryUrl();
        Context ctx = createContext();

        String output = handler.handleRequest(input, ctx);

        // TODO: validate output here if needed.
        Assert.assertEquals("Hello from SNS!", output);
    }
}
