#!/bin/bash
#################################################################################
# Target Table : scp tsv files to DM1
# Source Table :
#   yc_bit.ods_service_order            *
#   yc_bit.fo_service_order_ext         *
#   yc_bit.fo_service_order             *
#   yc_bit.ods_service_order_charge     *
#   yc_ods.fdcs_device                  only useful columns
# Refresh Frequency: 每日
# Version Info :
# 修改人   修改时间       修改原因
# ------   -----------    --------------------
# lujin    2017-02-23     规范化
#################################################################################
echo "中台0公里分析【订单】数据导出（每天凌晨5点导出前一天数据）"

# 基本变量
yesterday=$(date +%Y%m%d --date '1 days ago')

BDP_TEMP_DIR="/tmp/user_lujin"  # BDP hive导出的临时目录
TARGET_HOST="lujin@172.17.0.57:/home/lujin/tmp/"
TARGET_PSW="123Qwe,./"

WHERE=" where dt like '${yesterday}%' "
echo "[INFO] $WHERE"

dump()
{
  TMP_DIR="${BDP_TEMP_DIR}/$1"
  if [ -d $TMP_DIR ]; then
      echo "[DEBUG] clear dir $TMP_DIR"
      rm -fr $TMP_DIR
      mkdir -p $TMP_DIR
  else
      mkdir -p $TMP_DIR
  fi
  SQL="insert overwrite local directory '$TMP_DIR' row format delimited fields terminated by '\t' $2"
  echo "[DEBUG] $SQL"
  hive -e "${SQL}"

  OUTFILE="${TMP_DIR}_$yesterday.tsv"
  echo "[DEBUG] result dumped to $OUTFILE"
  cat $TMP_DIR/* > $OUTFILE
  echo "[DEBUG] sshpass -p ${TARGET_PSW} scp ${OUTFILE} lujin@172.17.0.57:/home/lujin/tmp/"
  sshpass -p ${TARGET_PSW} scp ${OUTFILE} lujin@172.17.0.57:/home/lujin/tmp/
  rm $OUTFILE
}

echo "[TRACE] STEP 1. dump yc_bit.ods_service_order"
DB="yc_bit"
TABLE="ods_service_order"
SELECT="select * from ${DB}.${TABLE}"
dump ${DB} ${SELECT}

echo "[TRACE] STEP 2. dump yc_bit.fo_service_order_ext"
DB="yc_bit"
TABLE="fo_service_order_ext"
SELECT="select * from ${DB}.${TABLE}"
dump ${DB} ${SELECT}

echo "[TRACE] STEP 3. dump yc_bit.fo_service_order"
DB="yc_bit"
TABLE="fo_service_order"
SELECT="select * from ${DB}.${TABLE}"
dump ${DB} ${SELECT}

echo "[TRACE] STEP 4. dump yc_bit.ods_service_order_charge"
DB="yc_bit"
TABLE="ods_service_order_charge"
SELECT="select * from ${DB}.${TABLE}"
dump ${DB} ${SELECT}

echo "[TRACE] STEP 5. dump yc_ods.fdcs_device (flitered) "
DB="yc_ods"
TABLE="fdcs_device"
COLS="device_id,user_id,status,user_type,update_time,app_version,os_type,os_version,mac_address,device_type,device_serialno,exception_flag,os_name,app_series,dt"
EXT_WHERE="$WHERE and status=1 and user_id>0"
SELECT="select $COLS from (select *, Row_Number() OVER (distribute by user_id sort BY update_time desc) rank from $DB.$TABLE $EXT_WHERE ) tmp where rank=1"
dump ${DB} ${SELECT}

echo "[NOTICE] all is done!"


