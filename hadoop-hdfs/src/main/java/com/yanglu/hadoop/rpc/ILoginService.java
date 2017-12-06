package com.yanglu.hadoop.rpc;

public interface ILoginService {

    public String login(String username, String password);

    public static final long versionID = 1L;
}
