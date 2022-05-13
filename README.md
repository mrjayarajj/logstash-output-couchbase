# Logstash Java Plugin

This is a Java plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

The documentation for Logstash Java plugins is available [here](https://www.elastic.co/guide/en/logstash/6.7/contributing-java-plugin.html).

## Step 1 : 
###### gradle.properties
```
LOGSTASH_CORE_PATH=/Users/Downloads/ELK/logstash-7.16.3
```

## Step 2 : 
###### build.sh
```
./gradlew gem
which will generate gem file
```

## Step 3 : 
###### installation to logstash
```
${logstash_path}/bin/logstash-plugin install --no-verify --local ${logstash_custom_plugin_path}/logstash-output-couchbase-1.0.1.gem
```


## Example : 
```
outupt { 
  couchbase {
            hosts => ["${couchbase_hosts_1}","${couchbase_hosts_2}"]
            bucket_name => "${couchbase_username}"
            bucket_password => "${couchbase_password}"
            document_id => "id"
            action => "update"
	    }
}
```
