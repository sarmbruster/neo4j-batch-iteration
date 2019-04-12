package org.neo4j.batchiteration;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.helpers.collection.PagingIterator;
import org.neo4j.procedure.*;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class Batching {

    @Context
    public GraphDatabaseService db;

    @Procedure(name = "com.wayne.loop", mode = Mode.WRITE)
    @Description("CALL com.wayne.loop(pagesize)")
//    public void loop(@Name("numThreads") Number numThreads) throws InterruptedException {
    public void loop(@Name("pageSize") long pageSize) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        Iterator<Node> nodes = db.getAllNodes().iterator();
        //.filter(n -> n.hasProperty("bucket"))

        PagingIterator<Node> pagingIterator = new PagingIterator<>(nodes, (int) pageSize);
        while (pagingIterator.hasNext()) {
            final Iterator<Node> page = pagingIterator.nextPage();
            final Collection<Node> materializedPage = Iterators.asCollection(page);
            executorService.submit(() -> {
                try (Transaction tx = db.beginTx()) {
                    for (Node node: materializedPage) {
                        Label origLabel = node.getLabels().iterator().next();
                        String newLabel = String.format("%s_%s", origLabel.name(), node.getProperty("username"));
                        node.addLabel(Label.label(newLabel));
                        node.removeLabel(origLabel);
                    }
                    tx.success();
                }
                log("done with processing page");
            });
            log("done with submitting page");
        }
        log("fully done submitting");
        executorService.shutdown();
        executorService.awaitTermination(1, TimeUnit.HOURS);
        log("fully done processing");
    }


    private static List<Node> POISON = Collections.EMPTY_LIST;

    @Procedure(name = "com.wayne.loopImproved", mode = Mode.WRITE)
    @Description("CALL com.wayne.loopImproved(pagesize)")
    public void loopImproved(@Name("pageSize") long pageSize) throws InterruptedException {

        int numberOfProcessors = Runtime.getRuntime().availableProcessors();
        final BlockingQueue<List<Node>> queue = new LinkedBlockingQueue<>(numberOfProcessors);
        final AtomicBoolean poisoned = new AtomicBoolean(false);

        List<Thread> threads = new ArrayList<>(numberOfProcessors);
        for (int i=0; i<numberOfProcessors; i++) {
            Thread thread = new Thread(() -> {
                try {
                    while ((poisoned.get()==false) || (!queue.isEmpty())) {
                        List<Node> page = queue.take();
                        try (Transaction tx = db.beginTx()) {
                            for (Node node : page) {
                                Label origLabel = node.getLabels().iterator().next();
                                String newLabel = String.format("%s_%s", origLabel.name(), node.getProperty("username"));
                                node.addLabel(Label.label(newLabel));
                                node.removeLabel(origLabel);
                            }
                            tx.success();
                        }
                        sleep(1000);
                        log("done with processing page");
                    }
                    log("being poisoned, terminating thread");
                } catch (InterruptedException e) {
                    log(e.getMessage());
                    throw new RuntimeException(e);
                }
            });
            thread.start();
            threads.add(thread);
        }

        Iterator<Node> nodes = db.getAllNodes().iterator();
        PagingIterator<Node> pagingIterator = new PagingIterator<>(nodes, (int) pageSize);
        while (pagingIterator.hasNext()) {
            final Iterator<Node> page = pagingIterator.nextPage();
            final List<Node> materializedPage = Iterators.asList(page);
            queue.put(materializedPage);
            log("done with submitting page, queue size is " + queue.size());
        }
        poisoned.set(true);
        log("fully done submitting, poison sent");
        for (Thread thread : threads) {
            thread.join();
        }
        log("fully done processing");
    }

    private static void log(String msg) {
        System.out.println(LocalDateTime.now() + " " + Thread.currentThread().getName() + " " + msg);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
