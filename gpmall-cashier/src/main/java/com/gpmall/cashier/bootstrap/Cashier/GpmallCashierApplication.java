package com.gpmall.cashier.bootstrap.Cashier;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = "com.gpmall.cashier.bootstrap")
@SpringBootApplication
public class GpmallCashierApplication {

    public static void main(String[] args) {
        SpringApplication.run(GpmallCashierApplication.class, args);
    }

}
