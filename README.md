# Logstash Java Plugin

This is a Java plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

The documentation for Logstash Java plugins is available [here](https://www.elastic.co/guide/en/logstash/6.7/contributing-java-plugin.html).

```
outupt { 
  couchbase {
            hosts =>                 ["${couchbase_hosts_1}","${couchbase_hosts_2}","${couchbase_hosts_3}","${couchbase_hosts_4}","${couchbase_hosts_5}","${couchbase_hosts_6}"]
            bucket_name => "${couchbase_username}"
            bucket_password => "${couchbase_password}"
            document_id => "id"
            action => "update"
	    }
}
```
