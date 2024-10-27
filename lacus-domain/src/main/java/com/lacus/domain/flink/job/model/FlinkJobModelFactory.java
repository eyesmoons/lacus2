package com.lacus.domain.flink.job.model;

import cn.hutool.core.bean.BeanUtil;
import com.lacus.domain.flink.job.command.AddFlinkJarJobCommand;
import com.lacus.domain.flink.job.command.AddFlinkSqlJobCommand;
import com.lacus.domain.flink.job.command.UpdateFlinkJarJobCommand;
import com.lacus.domain.flink.job.command.UpdateFlinkSqlJobCommand;

public class FlinkJobModelFactory {

    public static FlinkJobModel loadFromSqlAddCommand(AddFlinkSqlJobCommand addSqlCommand, FlinkJobModel model) {
        if (addSqlCommand != null && model != null) {
            BeanUtil.copyProperties(addSqlCommand, model);
        }
        return model;
    }

    public static FlinkJobModel loadFromSqlUpdateCommand(UpdateFlinkSqlJobCommand updateSqlCommand, FlinkJobModel model) {
        if (updateSqlCommand != null && model != null) {
            BeanUtil.copyProperties(updateSqlCommand, model);
        }
        return model;
    }

    public static FlinkJobModel loadFromJarAddCommand(AddFlinkJarJobCommand addJarCommand, FlinkJobModel model) {
        if (addJarCommand != null && model != null) {
            BeanUtil.copyProperties(addJarCommand, model);
        }
        return model;
    }

    public static FlinkJobModel loadFromJarUpdateCommand(UpdateFlinkJarJobCommand updateJarCommand, FlinkJobModel model) {
        if (updateJarCommand != null && model != null) {
            BeanUtil.copyProperties(updateJarCommand, model);
        }
        return model;
    }
}
