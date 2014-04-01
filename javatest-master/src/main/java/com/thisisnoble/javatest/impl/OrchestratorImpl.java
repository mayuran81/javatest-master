package com.thisisnoble.javatest.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.thisisnoble.javatest.Event;
import com.thisisnoble.javatest.Orchestrator;
import com.thisisnoble.javatest.Processor;
import com.thisisnoble.javatest.Publisher;

public class OrchestratorImpl implements Orchestrator {
	private List<Processor> processorList = new CopyOnWriteArrayList<Processor>();
	Publisher publisher;
	private static int MAX_THREADS = 50;
	ExecutorService taskHandlers = Executors.newFixedThreadPool(MAX_THREADS);
	List<Future<Event>> submitList = new ArrayList<Future<Event>>();
	CompositeEvent ce = null;
	boolean isParentEvent = true;

	@Override
	public void register(Processor processor) {

		processorList.add(processor);

	}

	@Override
	public void receive(final Event event)  {
		
		Future<Event> submit;
		synchronized (this) {
			if (isParentEvent) {
				ce = new CompositeEvent(event);
				isParentEvent = false;
			} else {
				ce.addChild(event);
			}
			final Iterator<Processor> it = processorList.iterator();

			while (it.hasNext()) {

				Callable<Event> c = new Callable<Event>() {
					final Processor process = it.next();

					@Override
					public Event call() throws Exception {

						if (process.interestedIn(event)) {

							process.process(event);

						}
						return event;
					}
				};
				submit = taskHandlers.submit(c);
				submitList.add(submit);
			}
			taskHandlers.shutdown();
			try {
				taskHandlers.awaitTermination(1000, TimeUnit.SECONDS);
				publisher.publish(ce);
				

			} catch (InterruptedException e1) {
				System.err.println(e1.getMessage());
			}

		

		}

	}

	@Override
	public void setup(Publisher publisher) {
		this.publisher = publisher;
	}

}
