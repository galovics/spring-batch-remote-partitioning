package com.arnoldgalovics.blog.springbatchremotepartitioningkafka.worker;

public class Customer {
    private int id;

    public Customer(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}
