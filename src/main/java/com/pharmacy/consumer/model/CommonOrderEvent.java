package com.pharmacy.consumer.model;

import lombok.Data;

@Data
public class CommonOrderEvent {
    private String patientId;
    private String patientName;
    private int age;
    private String gender;
    private String prescriptionId;
    private String medicineName;
    private String dosage;
    private int quantity;
    private String doctorName;
    private String orderDateTime;
    private String paymentMode;
    private String deliveryAddress;
}
