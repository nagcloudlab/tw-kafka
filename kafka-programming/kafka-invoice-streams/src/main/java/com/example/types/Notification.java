
package com.example.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.commons.lang.builder.ToStringBuilder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "InvoiceNumber",
    "CustomerCardNo",
    "TotalAmount",
    "EarnedLoyaltyPoints"
})
public class Notification {

    @JsonProperty("InvoiceNumber")
    private String invoiceNumber;
    @JsonProperty("CustomerCardNo")
    private String customerCardNo;
    @JsonProperty("TotalAmount")
    private Double totalAmount;
    @JsonProperty("EarnedLoyaltyPoints")
    private Double earnedLoyaltyPoints;

    @JsonProperty("InvoiceNumber")
    public String getInvoiceNumber() {
        return invoiceNumber;
    }

    @JsonProperty("InvoiceNumber")
    public void setInvoiceNumber(String invoiceNumber) {
        this.invoiceNumber = invoiceNumber;
    }

    @JsonProperty("CustomerCardNo")
    public String getCustomerCardNo() {
        return customerCardNo;
    }

    @JsonProperty("CustomerCardNo")
    public void setCustomerCardNo(String customerCardNo) {
        this.customerCardNo = customerCardNo;
    }

    @JsonProperty("TotalAmount")
    public Double getTotalAmount() {
        return totalAmount;
    }

    @JsonProperty("TotalAmount")
    public void setTotalAmount(Double totalAmount) {
        this.totalAmount = totalAmount;
    }

    @JsonProperty("EarnedLoyaltyPoints")
    public Double getEarnedLoyaltyPoints() {
        return earnedLoyaltyPoints;
    }

    @JsonProperty("EarnedLoyaltyPoints")
    public void setEarnedLoyaltyPoints(Double earnedLoyaltyPoints) {
        this.earnedLoyaltyPoints = earnedLoyaltyPoints;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("invoiceNumber", invoiceNumber).append("customerCardNo", customerCardNo).append("totalAmount", totalAmount).append("earnedLoyaltyPoints", earnedLoyaltyPoints).toString();
    }

}
