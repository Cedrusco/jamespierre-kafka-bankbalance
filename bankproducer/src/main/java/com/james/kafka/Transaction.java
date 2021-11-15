package com.james.kafka;

public class Transaction {
    public String name;
    public int amount;
    public String time;

    public Transaction(String name, int amount, String time) {
        this.name = name;
        this.amount = amount;
        this.time = time;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String value) {
        this.name = name;
    }

    public int getAmount() {
        return this.amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getTime() {
        return this.time;
    }

    public void setTime() {
        this.time = time;
    }
}
