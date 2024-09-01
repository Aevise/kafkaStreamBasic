package com.learnkafkastreams.exceptionHandler;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

@Slf4j
public class StreamProcessorCustomerErrorHandler implements StreamsUncaughtExceptionHandler {
    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
      log.error("Exception in the Application : {}}", exception.getMessage(), exception);

      if (exception instanceof StreamsException) {
          Throwable cause = exception.getCause();
          if(cause.getMessage().equals("Transient Error")){
//              return StreamThreadExceptionResponse.REPLACE_THREAD;
              return StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
          }
      }
        return StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
    }
}
