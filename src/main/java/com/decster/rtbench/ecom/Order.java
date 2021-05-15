package com.decster.rtbench.ecom;

public class Order {
    public long id;
    public long userId;
    public int goodId;
    public int merchantId;
    public String shipAddress;
    public String shipMode;
    public int orderDate;
    public int orderTs;
    public int paymentTs;
    public int deliveryStartTs;
    public int deliveryFinishTs;
    public int quantity;
    public int price;    // cent
    public int discount; // percent
    public int revenue;  // price * quantity * (100-discnt) / 100
    public int state;
    public int nextEventTs; // 0 means no more events
}
