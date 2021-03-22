package com.vnera.curator.example;

public interface Leases {
    String baseLeasePath = "/vrni/restapilayer/analytics";
    String masterLease = baseLeasePath + "/master/lock";
    String guardLease = baseLeasePath + "/guard/lock";
}
