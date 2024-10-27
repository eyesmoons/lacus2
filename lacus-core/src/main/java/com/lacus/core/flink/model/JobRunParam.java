package com.lacus.core.flink.model;

import com.lacus.enums.FlinkJobTypeEnum;
import lombok.Data;

@Data
public class JobRunParam {

  /**
   * sql语句目录
   */
  private String sqlPath;

  /**
   * 任务类型
   */
  private FlinkJobTypeEnum jobTypeEnum;

  /**
   * CheckPoint 参数
   */
  private CheckPointParam checkPointParam;

}
