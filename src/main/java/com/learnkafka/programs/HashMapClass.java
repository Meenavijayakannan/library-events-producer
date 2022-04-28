package com.learnkafka.programs;

import java.util.HashMap;

public class HashMapClass {

    public static void main(String[] args){
        HashMap<String,Integer> map = new HashMap<String, Integer>();
        map.put("first",1);
        map.put("second",2);

        System.out.println(map.get("second"));



    }


}
