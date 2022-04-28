package com.learnkafka.programs;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class MainApp {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context =
                new AnnotationConfigApplicationContext(AppConfig.class);


        UserService userService1 = context.getBean(UserService.class);
        System.out.println(userService1);
        userService1.setUserName("BORAJI.COM");
        System.out.println(userService1.getUserName());


        UserService userService2 = context.getBean(UserService.class);
        System.out.println(userService2);
        System.out.println(userService2.getUserName());
        context.close();

        if(userService1 == userService2){
            System.out.println(true);
        }
    }
}
