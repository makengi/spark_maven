package com.imr.spark.model;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@ToString
public class KafkaMessage implements Serializable {

    private static final long serialVersionUID = 1L;

    private String first;
    private String second;

    @Builder
    public KafkaMessage(String first,String second){
        this.first = first;
        this.second = second;
    }
}
