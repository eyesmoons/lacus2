package com.lacus.admin.controller.metadata;

import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.domain.metadata.db.DbService;
import com.lacus.domain.metadata.db.model.MetaDbModel;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Api(value = "数据库管理", tags = {"数据库管理"})
@RestController
@RequestMapping("/metadata/db")
public class DbController {

    @Autowired
    private DbService dbService;

    @ApiOperation("数据库列表")
    @GetMapping("/list/{datasourceId}")
    public ResponseDTO<List<MetaDbModel>> listByDatasourceId(@PathVariable("datasourceId") Long datasourceId) {
        List<MetaDbModel> list = dbService.listByDatasourceId(datasourceId);
        return ResponseDTO.ok(list);
    }
}
