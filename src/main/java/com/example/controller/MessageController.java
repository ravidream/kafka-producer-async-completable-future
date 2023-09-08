package com.example.controller;

import java.io.IOException;
import java.io.StringWriter;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.dto.MessageDTO;
import com.example.producer.KafkaMessageProducer;
import com.fasterxml.jackson.core.exc.StreamWriteException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

@RestController
@RequestMapping("/api")
public class MessageController {
	
	@Autowired
	KafkaMessageProducer kafkaMessageProducer;
	
	@PostMapping(value = "/create")
	public String createMessage(@RequestBody MessageDTO messageDTO) throws StreamWriteException, DatabindException, IOException, InterruptedException, ExecutionException {
		ObjectMapper objectMapper = new ObjectMapper();
        StringWriter stringVal = new StringWriter();
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
        objectMapper.writeValue(stringVal, messageDTO);
        CompletableFuture<SendResult<String, String>> completableFuture = kafkaMessageProducer.sendMessage(stringVal.toString());
		return completableFuture.get().getProducerRecord().value();
	}
}
