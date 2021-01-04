package com.wufuqiang.entities;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class Website {
    private int id;
    private String name;
    private String url;
    private int alexa;
    private String country;

}
