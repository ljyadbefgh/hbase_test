package com.ljy.hadoop.study.hbase_test.demo.model;

import java.util.Date;

public class Student {
    private String id;
    private String name;
    private Date birth;

    public Student() {
    }

    public Student(String id, String name, Date birth) {
        this.id = id;
        this.name = name;
        this.birth = birth;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getBirth() {
        return birth;
    }

    public void setBirth(Date birth) {
        this.birth = birth;
    }
}
