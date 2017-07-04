DROP TABLE
IF EXISTS streaming_error_info;

CREATE TABLE `streaming_error_info` (
	`ID` INT (11) NOT NULL AUTO_INCREMENT COMMENT '序号',
	`collectTime` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' COMMENT '时间',
	`task_name` CHAR (40) COMMENT '任务名称',
	`exception_info` text COMMENT '异常描述',
	`json` text COMMENT '数据',
	PRIMARY KEY (`ID`)
);