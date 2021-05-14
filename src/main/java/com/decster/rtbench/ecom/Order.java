package com.decster.rtbench.ecom;

public class Order {
    public long id;
    public byte linenumber;
    public int userId;
    public int goodId;
    public int merchantId;
    public int orderDate;
    public int orderTs;
    public int paymentTs;
    public int deliveryStartTs;
    public int deliveryFinishTs;
    public int quantity;
    public int price;    // cent
    public int discount; // percent
    public int revenue;  // price * quantity * (100-discnt) / 100
    public int totalRevenue;
    public String shipMode;
    public int state;
}
