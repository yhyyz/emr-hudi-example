# 创建表
sysbench creates.lua \
    --mysql-user=admin --mysql-password=panchao0129#$  \
    --mysql-host=news-test.cszxtewro2cv.ap-southeast-1.rds.amazonaws.com \
    --mysql-db=cdc_test_db --report-interval=1 \
    --events=1 run

# 插入数据
sysbench insert.lua \
    --mysql-user=admin --mysql-password=panchao0129#$  \
    --mysql-host=news-test.cszxtewro2cv.ap-southeast-1.rds.amazonaws.com \
    --mysql-db=cdc_test_db --report-interval=1 \
    --events=500000 --time=0 --threads=10  --skip_trx=true run

# 更新数据
sysbench update.lua \
    --mysql-user=admin --mysql-password=panchao0129#$  \
    --mysql-host=news-test.cszxtewro2cv.ap-southeast-1.rds.amazonaws.com \
    --mysql-db=cdc_test_db --report-interval=1 \
    --events=10000 --time=0 --threads=10 --skip_trx=true --update_id_min=1 --update_id_max=200000 run