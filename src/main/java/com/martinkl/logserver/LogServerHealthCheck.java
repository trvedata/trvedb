package com.martinkl.logserver;

import com.codahale.metrics.health.HealthCheck;

public class LogServerHealthCheck extends HealthCheck {

    @Override
    protected Result check() throws Exception {
        //return Result.unhealthy("explanation");
        return Result.healthy();
    }
}
