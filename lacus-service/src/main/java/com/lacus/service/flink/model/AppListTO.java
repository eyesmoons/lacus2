package com.lacus.service.flink.model;

import lombok.Data;

import java.util.List;

@Data
public class AppListTO {

  private List<AppTO> app;
}