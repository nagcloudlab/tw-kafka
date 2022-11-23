package com.example;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Consumer;
import java.util.function.Supplier;

@SpringBootApplication
public class SpringCloudStreamQuickstartApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringCloudStreamQuickstartApplication.class, args);
	}

	// supplier-out-0 -> supplier-out-0
//	@Bean
//	public Supplier<String> supplier(){
//		return () -> "Hello World";
//	}

	@Bean
	public Consumer<String> consumer(){
		return message -> {
			System.out.println("Received: " + message);
		};
	}

}
