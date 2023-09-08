package com.example.producer;

import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

@Component
public class KafkaMessageProducer {
	
	private static Logger LOGGER = LoggerFactory.getLogger(KafkaMessageProducer.class);
	
	private static final String TOPIC = "test";
	
	@Autowired
	@Qualifier("producer")
	private KafkaTemplate<String, String> kafkaTemplate;

	@Async("asyncExecutor")
	public CompletableFuture<SendResult<String, String>> sendMessage(String message) {
		
		CompletableFuture<SendResult<String, String>> future = new CompletableFuture<>();
	    try {
	        CompletableFuture<SendResult<String, String>> completableFuture = this.kafkaTemplate.execute(ctx -> {
	            return kafkaTemplate.send(TOPIC, message);
	        }).completable();
	        completableFuture.whenComplete((s, ex) -> {
	            if (ex != null) {
	                future.completeExceptionally(ex);
	            }
	            else {
	                future.complete(s);
	            }
	        });
	    }
	    catch (Exception ex) {
	        future.completeExceptionally(ex);
	    }
		return future;
	}
}