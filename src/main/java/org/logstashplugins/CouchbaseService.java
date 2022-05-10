package org.logstashplugins;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.couchbase.client.core.BackpressureException;
import com.couchbase.client.core.DocumentConcurrentlyModifiedException;
import com.couchbase.client.core.RequestCancelledException;
import com.couchbase.client.core.time.Delay;
import com.couchbase.client.core.tracing.ThresholdLogReporter;
import com.couchbase.client.core.tracing.ThresholdLogTracer;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.error.DurabilityException;
import com.couchbase.client.java.error.TemporaryFailureException;
import com.couchbase.client.java.util.retry.RetryBuilder;

import io.opentracing.Tracer;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;

@Slf4j
public class CouchbaseService {

	private CouchbaseCluster cluster = null;

	private AsyncBucket asyncBucket = null;

	public CouchbaseService(List<Object> hosts, String bucketName, String bucketPassword) {
		try {

			long connectTimeout = 10000;
			long kvTimeout = 200;
			long bucketTimeout = 5000;

			Tracer tracer = ThresholdLogTracer.create(ThresholdLogReporter.builder() // builder
					.kvThreshold(kvTimeout, TimeUnit.SECONDS) // kv timeout
					.logInterval(1, TimeUnit.MINUTES) // log every 15 min
					.sampleSize(Integer.MAX_VALUE) // sample size
					.pretty(true) // pretty print the json output in the logs
					.build());

			List<String> nodes = hosts.stream().map(h -> (String) h).collect(Collectors.toList());
			CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().tracer(tracer)
					.connectTimeout(connectTimeout).kvTimeout(kvTimeout).sslEnabled(false).build();
			cluster = CouchbaseCluster.create(env, nodes);
			asyncBucket = cluster.openBucket(bucketName, bucketPassword, bucketTimeout, TimeUnit.SECONDS).async();

		} catch (Exception e) {
			log.error("Not able to connect to couchbase : ", e);
			throw e;
		}
	}

	public Observable<JsonDocument> upsert(String documentId, Map<String, Object> documentValue) {
		JsonDocument doc = JsonDocument.create(documentId, JsonObject.empty().from(documentValue));
		return retry(asyncBucket.upsert(doc));
	}

	public Observable<JsonDocument> update(String documentId, Map<String, Object> documentValue) {

		return asyncBucket.get(documentId).defaultIfEmpty(null).flatMap(eDoc -> {			
			log.debug(Thread.currentThread().getName()+" : Doc Id="+documentId+" "+(eDoc!=null ? "exist" : "not exist"));
			if (eDoc != null) {
				Map<String, Object> fields = eDoc.content().toMap();
				fields.putAll(documentValue);
				JsonDocument doc = JsonDocument.create(documentId, JsonObject.empty().from(fields));
				return retry(asyncBucket.upsert(doc));
			} else {
				JsonDocument doc = JsonDocument.create(documentId, JsonObject.empty().from(documentValue));
				return retry(asyncBucket.insert(doc));
			}
		});
	}

	@SuppressWarnings("unchecked")
	private Observable<JsonDocument> retry(Observable<JsonDocument> o) {
		return o.retryWhen(//
				RetryBuilder.anyOf(DurabilityException.class, // DurabilityException
						DocumentConcurrentlyModifiedException.class, // DocumentConcurrentlyModifiedException
						TemporaryFailureException.class, // TemporaryFailureException
						RequestCancelledException.class, // RequestCancelledException
						BackpressureException.class // BackpressureException
				).delay(Delay.linear(TimeUnit.MILLISECONDS, 5000, 1, 500)) // wait before retry
						.max(100) // retry 100 times
						.build());
	}

	public void shutdown() {

		if (asyncBucket != null && !asyncBucket.isClosed()) {
			asyncBucket.close();
		}

		if (cluster != null) {
			cluster.disconnect();
		}

	}

}
