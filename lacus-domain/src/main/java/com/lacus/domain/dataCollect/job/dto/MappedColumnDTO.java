package com.lacus.domain.dataCollect.job.dto;

import lombok.Data;

import java.util.LinkedList;

@Data
public class MappedColumnDTO {
    private String jobId;
    private LinkedList<ColumnDTO> mappedSourceColumns;
    private LinkedList<ColumnDTO> mappedSinkColumns;
}
