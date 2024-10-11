package com.example.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.core.GenericHandler;
import org.springframework.integration.dsl.DirectChannelSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.MessageChannels;
import org.springframework.integration.dsl.PollerFactory;
import org.springframework.integration.jdbc.JdbcPollingChannelAdapter;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.Map;

@SpringBootApplication
public class ConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }


    @Bean
    JdbcPollingChannelAdapter jdbcPollingChannelAdapter(DataSource dataSource) {
        var jdbc = new JdbcPollingChannelAdapter(dataSource, "select * from customer");
        jdbc.setRowMapper((rs, rowNum) ->
                new Customer(rs.getInt("id"), rs.getString("name")));
        return jdbc;
    }


    @Bean
    IntegrationFlow customerHandlingFlow(MessageChannel customers) {
        return IntegrationFlow
                .from(customers)
                .handle((GenericHandler<Customer>) (payload, headers) -> {
                    System.out.println("customer [" + payload + "]");
                    return null;
                })
                .get();
    }

    @Bean
    IntegrationFlow jdbcInboundFlow(
            MessageChannel customers,
            JdbcPollingChannelAdapter jdbcPollingChannelAdapter) {
        return IntegrationFlow
                .from(jdbcPollingChannelAdapter, poller -> poller.poller(pm -> PollerFactory.fixedRate(Duration.ofSeconds(1))))
                .split()
                .channel(customers)
                .split()
                .get();
    }

    @Bean
    DirectChannelSpec customers() {
        return MessageChannels.direct();
    }
}

@Controller
@ResponseBody
class CustomerController {

    private final MessageChannel customers;

    CustomerController(MessageChannel customers) {
        this.customers = customers;
    }

    @PostMapping("/customers")
    void process(@RequestBody Map<String, Object> customer) {
        var message = MessageBuilder.withPayload(new Customer((Integer) customer.get("id"),
                (String) customer.get("name"))).build();
        this.customers.send(message);
    }
}

record Customer(int id, String name) {
}
