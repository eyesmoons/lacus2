package com.lacus.sink.impl;

import com.lacus.sink.ISink;
import lombok.Getter;

/**
 * sink抽象处理器
 * @author shengyu
 * @date 2024/4/30 15:56
 */
@Getter
public abstract class BaseSink implements ISink {

    protected String name;

    public BaseSink(String name) {
        this.name = name;
    }
}
