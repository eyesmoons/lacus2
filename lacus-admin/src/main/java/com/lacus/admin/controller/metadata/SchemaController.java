package com.lacus.admin.controller.metadata;

import com.lacus.common.core.dto.ResponseDTO;
import com.lacus.domain.metadata.schema.SyncSchemaService;
import com.lacus.domain.metadata.schema.command.SyncDbTablesCommand;
import com.lacus.domain.metadata.schema.dto.SchemaDbDTO;
import com.lacus.domain.metadata.schema.dto.SchemaTableDTO;
import com.lacus.domain.metadata.schema.model.SchemaDbTreeNode;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Api(value = "元同步数据", tags = {"元同步数据"})
@RestController
@RequestMapping("/metadata/schema")
public class SchemaController {

    @Autowired
    private SyncSchemaService syncSchemaService;

    @ApiOperation("schemaDb列表")
    @PreAuthorize("@permission.has('metadata:schema:list')")
    @GetMapping("/listSchemaDb/{datasourceId}")
    public ResponseDTO<SchemaDbTreeNode> listSchemaDb(@PathVariable("datasourceId") Long datasourceId) {
        SchemaDbTreeNode node = syncSchemaService.getSchemaDbTree(datasourceId);
        return ResponseDTO.ok(node);
    }

    @ApiOperation("schemaTable列表")
    @PreAuthorize("@permission.has('metadata:schema:list')")
    @GetMapping("/listSchemaTable/{datasourceId}/{dbName}")
    public ResponseDTO<List<SchemaTableDTO>> listSchemaTable(@PathVariable("datasourceId") Long datasourceId, @PathVariable("dbName") String dbName) {
        List<SchemaTableDTO> schemaDbList = syncSchemaService.getSchemaTableList(datasourceId, dbName, null);
        return ResponseDTO.ok(schemaDbList);
    }

    @ApiOperation("同步dbTables")
    @PreAuthorize("@permission.has('metadata:schema:edit')")
    @PostMapping("/syncDbTables")
    public ResponseDTO<Boolean> syncDbTables(@RequestBody SyncDbTablesCommand command) {
        return ResponseDTO.ok(syncSchemaService.syncDbTables(command.getDatasourceId(), command.getDbTables()));
    }
}
