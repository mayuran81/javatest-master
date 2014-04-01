package com.thisisnoble.javatest;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.thisisnoble.javatest.events.MarginEvent;
import com.thisisnoble.javatest.events.RiskEvent;
import com.thisisnoble.javatest.events.ShippingEvent;
import com.thisisnoble.javatest.events.TradeEvent;
import com.thisisnoble.javatest.impl.CompositeEvent;
import com.thisisnoble.javatest.impl.OrchestratorImpl;
import com.thisisnoble.javatest.processors.MarginProcessor;
import com.thisisnoble.javatest.processors.RiskProcessor;
import com.thisisnoble.javatest.processors.ShippingProcessor;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SimpleOrchestratorTest {
	

    @Test
    public void tradeEventShouldTriggerAllProcessors() {
    	
    	//This could have gone into @Before of unit test class
        TestPublisher testPublisher = new TestPublisher();
        Orchestrator orchestrator = setupOrchestrator();
        orchestrator.setup(testPublisher);

        TradeEvent te = new TradeEvent("trade1", 1000.0);
        orchestrator.receive(te);
        safeSleep(100);      
        CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent();        
        
        assertEquals(te, ce.getParent());
        assertEquals(3, ce.size());
        for (Event evt : ce.getChildren()) {
            if (evt instanceof RiskEvent) {
                assertEquals(500.0, ((RiskEvent) evt).getRiskValue(), 0.01);
            } else if (evt instanceof ShippingEvent) {
                assertEquals(100.0, ((ShippingEvent) evt).getShippingCost(), 0.01);
            } else if (evt instanceof MarginEvent) {
                assertEquals(100.0, ((MarginEvent) evt).getMargin(), 0.01);
            }
        }
    }

    @Test
    public void shippingEventShouldTriggerOnly2Processors() {
        TestPublisher testPublisher = new TestPublisher();
        
        Orchestrator orchestrator = setupOrchestrator();
        orchestrator.setup(testPublisher);

        ShippingEvent se = new ShippingEvent("ship1", 200.0);
        orchestrator.receive(se);
        safeSleep(100);
        CompositeEvent ce = (CompositeEvent) testPublisher.getLastEvent();
        assertEquals(se, ce.getParent());
        assertEquals(2, ce.size());
        for (Event evt : ce.getChildren()) {
            if (evt instanceof ShippingEvent) {
                assertEquals(210.0, ((ShippingEvent) evt).getShippingCost(), 0.01);
            } else if (evt instanceof MarginEvent) {
                assertEquals(20.0, ((MarginEvent) evt).getMargin(), 0.01);
            }
        }
        
    }
    
    
    @Test
    public void mulipleEventTrigger() {
    	List<Future<Integer>> submitList=new ArrayList<Future<Integer>>();
        Future<Integer> submit;
        ExecutorService taskHandlers = Executors.newFixedThreadPool(2);
    	TestPublisher testPublisher = new TestPublisher();
    	TestPublisher tradePublisher = new TestPublisher();
    	Orchestrator orchestrator = setupOrchestrator();
    	Orchestrator tradeOrchestrator = setupOrchestrator();
    	tradeOrchestrator.setup(tradePublisher);
    	orchestrator.setup(testPublisher);
    	final ShippingEvent se = new ShippingEvent("ship1",200);
    	final TradeEvent te = new TradeEvent("trade1", 1000.0);
    	final Orchestrator finalOrchastretor =orchestrator;
    	final Orchestrator finalTradeOrchastretor =tradeOrchestrator;
    	Callable<Integer> shippingTask = new Callable<Integer>() {

			@Override
			public Integer call() throws Exception {
				finalOrchastretor.receive(se);
				return 1;
			}
    		
    		
		};
		Callable<Integer> tradingTask = new Callable<Integer>() {

			@Override
			public Integer call() throws Exception {
				finalTradeOrchastretor.receive(te);
				return 1;
			}
    		
    		
		};
		submit = taskHandlers.submit(shippingTask);
		submitList.add(submit);
		submit = taskHandlers.submit(tradingTask);
	
		taskHandlers.shutdown();
		try {
			taskHandlers.awaitTermination(100, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
    	safeSleep(100);
    	CompositeEvent ce = (CompositeEvent)testPublisher.getLastEvent();
    	CompositeEvent ce1 = (CompositeEvent)tradePublisher.getLastEvent();
    	assertEquals(se,ce.getParent());
    	assertEquals(te,ce1.getParent());
    	assertEquals(2, ce.size());
    	 for (Event evt : ce.getChildren()) {
             if (evt instanceof ShippingEvent) {
                 assertEquals(210.0, ((ShippingEvent) evt).getShippingCost(), 0.01);
             } else if (evt instanceof MarginEvent) {
                 assertEquals(20.0, ((MarginEvent) evt).getMargin(), 0.01);
             }
         }
    }
    
    
 
  

    private Orchestrator setupOrchestrator() {
        Orchestrator orchestrator = createOrchestrator();
        orchestrator.register(new RiskProcessor(orchestrator));
        orchestrator.register(new MarginProcessor(orchestrator));
        orchestrator.register(new ShippingProcessor(orchestrator));
        return orchestrator;
    }

    private void safeSleep(long l) {
        try {
            Thread.sleep(l);
        } catch (InterruptedException e) {
            //ignore
        }
    }

    private Orchestrator createOrchestrator() {
        //TODO solve the test
    	Orchestrator orchestrator = new OrchestratorImpl();
    	return orchestrator;
       
    }
}
