DROP TABLE IF EXISTS `ods_device`;
CREATE TABLE `ods_device`(
  `device_id` bigint,
  `user_id` bigint,
  `status` int,
  `user_type` varchar(20),
  `update_time` int,
  `app_version` varchar(20),
  `os_type` varchar(20),
  `os_version` varchar(20),
  `mac_address` varchar(20),
  `device_type` varchar(20),
  `device_serialno` varchar(20),
  `exception_flag` int,
  `os_name` int,
  `app_series` int,
  `dt` int,
  PRIMARY KEY (`device_id`),
  UNIQUE KEY `device_id` (`device_id`) USING HASH
) ENGINE=myisam DEFAULT CHARSET=utf8;


--从Hive取当天最新设备信息信息
select device_id,user_id,status,user_type,update_time,app_version,os_type,os_version,mac_address,device_type,device_serialno,exception_flag,os_name,app_series,dt
from (
	select *, Row_Number() OVER (distribute by user_id sort BY update_time desc) rank
	from yc_ods.fdcs_device
	where dt=20170207 and status=1 and user_id>0
) tmp
where rank=1
