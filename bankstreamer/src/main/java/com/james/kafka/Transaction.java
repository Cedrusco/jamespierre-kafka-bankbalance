package com.james.kafka;

import java.io.Serializable;

public class Transaction implements Serializable {
    public String name;
    public long amount;
    public String time;

    public Transaction(String name, long amount, String time) {
        this.name = name;
        this.amount = amount;
        this.time = time;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAmount() {
        return this.amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public String getTime() {
        return this.time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
