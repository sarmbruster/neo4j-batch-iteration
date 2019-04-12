package org.neo4j.batchiteration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BatchingTest {

    private GraphDatabaseService db;

    @Before
    public void setup() throws KernelException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Procedures procedures = ((GraphDatabaseAPI)db).getDependencyResolver().resolveDependency(Procedures.class, DependencyResolver.SelectionStrategy.ONLY);
        procedures.registerProcedure(Batching.class);
        db.execute("unwind range(1,10000) as x create (:Person{username:'person_' + x % 10})");
    }

    @After
    public void teardown() {
        db.shutdown();
    }

    @Test
    public void testLoopProcedure() {
        db.execute("CALL com.wayne.loop(1000)");

        Result result = db.execute("match (n) return labels(n)[0] as label, count(*) as count");
        List<Map<String, Object>> maps = Iterators.asList(result);

        assertEquals(10, maps.size());
        maps.forEach(map -> {
            assertEquals(1000l, map.get("count"));
        });
    }

    @Test
    public void testLoopImprovedProcedure() {
        db.execute("CALL com.wayne.loopImproved(1000)");

        Result result = db.execute("match (n) return labels(n)[0] as label, count(*) as count");
        List<Map<String, Object>> maps = Iterators.asList(result);

        assertEquals(10, maps.size());
        maps.forEach(map -> {
            assertEquals(1000l, map.get("count"));
        });
    }
}
