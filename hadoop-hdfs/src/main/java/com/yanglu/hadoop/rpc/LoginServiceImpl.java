package com.yanglu.hadoop.rpc;

public class LoginServiceImpl implements ILoginService {


    @Override
    public String login(String username, String password) {


        return username + " logged in successfully!";
    }
}
