package org.logstashplugins;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CouchbaseServiceTest {

	@Test
	public void testCouchbaseService() {

		Map<String, Object> doc = new HashMap<>();

		Map<String, Object> hvc = new HashMap<>();
		hvc.put("lastPurchaseDate", "2021-01-31");
		hvc.put("customerSegment", "2_Mid_Value");
		hvc.put("atRiskFlag", "N");
		hvc.put("customerCategory", "c. BRONZE");
		hvc.put("recency", "a. 0 TO 4 MONTHS");
		hvc.put("fequency", "e. 1-7 ORDERS/YEAR");

		doc.put("rewardsNumber", "0000748277");
		doc.put("hvc", hvc);
		doc.put("@timestamp", ""+new Date());

		String update = "0000748277";
		String insert = "8955878973";

		new CouchbaseService(Arrays.asList("localhost"), "rewardsContractProfile", "rewardsContractProfile").update(update, doc)
				.toBlocking().subscribe(v -> {
					log.info("event :" + doc);
				}, e -> {
					System.out.println(e);
					log.error(e.getMessage() + " Occured while processing event :" + doc);
				});

		// assertNotNull(upsertedDoc.toBlocking().first().cas());
	}

}
