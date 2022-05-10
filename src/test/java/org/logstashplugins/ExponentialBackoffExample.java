/*
 * Copyright (c) 2017 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.logstashplugins;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.util.retry.RetryBuilder;
import rx.Observable;
import rx.exceptions.CompositeException;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An example of using the retryWhen() to do an exponential backoff and retry on a BackpressureException.
 *
 * To simulate backpressure, this example grabs all of the routes for the travel-sample (since there are a lot of them)
 * and turns the buffer size down to intentionally run into a backpressure exception. It will log and increment a
 * a counter when backing off.
 *
 * If you adjust the request buffer size larger, you'll see less backing off.  Lower, you see more.  The run time
 * is also longer with more backing off.
 *
 *
 */
public class ExponentialBackoffExample {

    public static void main(String args[]) {

        CouchbaseEnvironment env = (CouchbaseEnvironment) DefaultCouchbaseEnvironment
                .builder()
                //.requestBufferSize(2) // must be a power of 2
                .build();
        Cluster cluster = CouchbaseCluster.create(env, "localhost");

        // Initialize the Connection
        cluster.authenticate("travel-sample", "travel-sample");  // this is for 5.0 style RBAC auth
        Bucket bucket = cluster.openBucket("travel-sample");

        ArrayList<String> theKeys = new ArrayList<String>();

        // fetch a bunch of known keys to try to fetch all at once
        N1qlQueryResult airlines = bucket.query(N1qlQuery.simple(
                "select meta().id from `travel-sample`"));

        for (N1qlQueryRow n1qlQueryRow :airlines.allRows()) {
        	System.out.println("n1qlQueryRow : "+n1qlQueryRow);
            theKeys.add(n1qlQueryRow.value().getString("id"));
        }

        
        AtomicInteger backedOff = new AtomicInteger(0);


        Observable.from(theKeys)
                .flatMap(id -> {
                    return
                    bucket.async().get(id)
                            .doOnError(err ->
                                {
                                    if (err instanceof BackpressureException) {
                                        System.err.println("Backpressure!");
                                        backedOff.incrementAndGet();
                                    }
                            })
//                            .timeout(100, TimeUnit.MICROSECONDS)
                            .retryWhen(RetryBuilder.anyOf(BackpressureException.class)
                                    .max(16)
                                    .delay(Delay.exponential(TimeUnit.MICROSECONDS, 100, 2))
                                    .build());
                })
                .last()
                .toBlocking()
                .subscribe(
                        System.out::println,
                        throwable -> {
                            if (throwable instanceof BackpressureException) {
                                System.out.println("Got back pressure exception");
                            } if (throwable instanceof CompositeException) {
                                System.err.println("Got composite exception containing…");
                                for (Throwable t : ((CompositeException) throwable).getExceptions()) {
                                    System.err.println("Exception: " + t.getCause() + " – " + t.getMessage());
                                    t.printStackTrace(System.err);
                                }
                            } else {
                                System.err.println("Unexpected throwable: " + throwable.getClass().getName());

                            }
                        });

        System.out.println("Run complete.  Times backed off for backpressure: " + backedOff.get());
        

    }

}
