package org.logstashplugins;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.jruby.RubyFixnum;
import org.jruby.RubyNil;
import org.jruby.RubyString;
import org.logstash.ConvertedList;
import org.logstash.ConvertedMap;
import org.logstash.ext.JrubyTimestampExtLibrary.RubyTimestamp;

import com.couchbase.client.java.document.JsonDocument;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Event;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.Output;
import co.elastic.logstash.api.Password;
import co.elastic.logstash.api.PluginConfigSpec;
import lombok.extern.slf4j.Slf4j;
import rx.Observable;

// class name must match plugin name
@LogstashPlugin(name = "couchbase")
@Slf4j
public class Couchbase implements Output {

	public static final PluginConfigSpec<List<Object>> HOSTS_CONFIG = PluginConfigSpec.arraySetting("hosts", null,false, true);
	public static final PluginConfigSpec<String> BUCKET_NAME_CONFIG = PluginConfigSpec.requiredStringSetting("bucket_name");
	public static final PluginConfigSpec<Password> BUCKET_PASSWORD_CONFIG = PluginConfigSpec.passwordSetting("bucket_password", null, false, true);
	public static final PluginConfigSpec<String> DOCUMENT_ID_CONFIG = PluginConfigSpec.requiredStringSetting("document_id");
	public static final PluginConfigSpec<String> ACTION_CONFIG = PluginConfigSpec.stringSetting("action", "update");

	private final String id;

	private String documentIdKey;
	private List<Object> hosts;
	private String bucketName;
	private Password bucketPassword;
	private String action;

	private CouchbaseService couchbaseService;

	private final CountDownLatch done = new CountDownLatch(1);
	private volatile boolean stopped = false;

	// all plugins must provide a constructor that accepts id, Configuration, and
	// Context
	public Couchbase(final String id, final Configuration configuration, final Context context) {
		this(id, configuration, context, System.out);
	}

	Couchbase(final String id, final Configuration config, final Context context, OutputStream targetStream) {
		// constructors should validate configuration options
		this.id = id;
		hosts = config.get(HOSTS_CONFIG);
		bucketName = config.get(BUCKET_NAME_CONFIG);
		bucketPassword = config.get(BUCKET_PASSWORD_CONFIG);
		documentIdKey = config.get(DOCUMENT_ID_CONFIG);
		action = config.get(ACTION_CONFIG);

		log.info("");
		log.info("logstash-output-couchbase version : 1.0.1, Added support for array or list type ");
		log.info("hosts:" + hosts);
		log.info("Bucket Name:" + bucketName);
		log.info("Document Id Key:" + documentIdKey);
		log.info("");

		couchbaseService = new CouchbaseService(hosts, bucketName, bucketPassword.getPassword());

	}

	private Map<String, Object> asJava(ConvertedMap covertedMap) {

		Map<String, Object> convertedJavaMap = new HashMap<>();

		covertedMap.keySet().forEach(key -> {
			if (covertedMap.get(key) instanceof RubyNil) {
				convertedJavaMap.put(key, null);
			} else if (covertedMap.get(key) instanceof RubyFixnum) {
				RubyFixnum str = (RubyFixnum) covertedMap.get(key);
				convertedJavaMap.put(key, str.getLongValue());
			} else if (covertedMap.get(key) instanceof ConvertedList) {
				ConvertedList list = (ConvertedList) covertedMap.get(key);
				convertedJavaMap.put(key, list.unconvert());
			} else if (covertedMap.get(key) instanceof RubyString) {
				RubyString str = (RubyString) covertedMap.get(key);
				convertedJavaMap.put(key, str.asJavaString());
			} else if (covertedMap.get(key) instanceof RubyTimestamp) {
				RubyTimestamp ts = (RubyTimestamp) covertedMap.get(key);
				convertedJavaMap.put(key, ts.toString());
			} else if (covertedMap.get(key) instanceof ConvertedMap) {
				convertedJavaMap.put(key, asJava((ConvertedMap) covertedMap.get(key)));
			} else {
				convertedJavaMap.put(key, covertedMap.get(key));
			}
		});

		return convertedJavaMap;
	}

	@Override
	public void output(final Collection<Event> events) {

		Iterator<Event> z = events.iterator();

		while (!events.isEmpty() && z.hasNext() && !stopped) {

			Event event = z.next();

			String documentIdValue = event.getField(documentIdKey).toString();

			ConvertedMap convertedMap = (ConvertedMap) event.getData();

			Map<String, Object> convertedJavaMap = asJava(convertedMap);

			Observable<JsonDocument> observableJsonDocument = null;

			if (action.equals("upsert")) {
				observableJsonDocument = couchbaseService.upsert(documentIdValue, convertedJavaMap);
			} else if (action.equals("update")) {
				observableJsonDocument = couchbaseService.update(documentIdValue, convertedJavaMap);
			}

			observableJsonDocument.toBlocking().subscribe(v -> {
			}, e -> {
				log.error(e.getMessage() + " Occured while processing event :" + convertedJavaMap);
			});

		}
	}

	@Override
	public void stop() {
		couchbaseService.shutdown();
		stopped = true;
		done.countDown();
	}

	@Override
	public void awaitStop() throws InterruptedException {
		done.await();
	}

	@Override
	public Collection<PluginConfigSpec<?>> configSchema() {
		// should return a list of all configuration options for this plugin
		return Arrays.asList(HOSTS_CONFIG, BUCKET_NAME_CONFIG, BUCKET_PASSWORD_CONFIG, DOCUMENT_ID_CONFIG,
				ACTION_CONFIG);
	}

	@Override
	public String getId() {
		return id;
	}

}
