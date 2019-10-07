# -*- coding: utf8 -*-
# 必要扩展包
import json
import pymysql as pms
from time import strftime as ft, time, localtime as lt, sleep
from datetime import date



# 连接到数据库 数据库参数见 start.init 主要参数设置 主机名/用户名/密码/数据库/字符集
def db_connect():
    try:
        pmt = ["rm-8vb78ejw3050i8u870o.mysql.zhangbei.rds.aliyuncs.com","jincomed","Jinke@2017","","utf8"]
        db = pms.connect(host=pmt[0], user=pmt[1], passwd=pmt[2], db="h1", charset=pmt[4])
    except BaseException as inst:
        print(inst)
    else:
        print("{} Database Connected!".format(ft("%Y-%m-%d %H:%M:%S", lt(time()))))
        return db


# 数据库查询 db 数据库名/ ds 查询天数/ cs 案例类型
def db_query(db, ds, cs):
    if ds == 7:
        boolean = "TRUE"
    else:
        boolean = "FALSE"
    if cs > 6:
        cmp = '>'
    elif cs == 6:
        cmp = '='
    else:
        cmp = '<'
    try:
        cursor = db.cursor()
        sql = \
            '''
        SELECT
            DATE(createtime) create_time,
            COUNT(*) count
        FROM
            cases{year}
        WHERE
            hospitalid = 'H10000107'
        AND (casestate {cmp_char} 6 OR {b})
        AND {now_days} - TO_DAYS(createtime) <= {days} - 1
        GROUP BY
            create_time
        UNION
        SELECT
            DATE(createtime) create_time,
            COUNT(*) count
        FROM
            cases{laseyear}
        WHERE
            hospitalid = 'H10000107'
        AND (casestate {cmp_char} 6 OR {b})
        AND {now_days} - TO_DAYS(createtime) <= {days} - 1
        GROUP BY
            create_time
        ORDER BY
            create_time DESC
            '''.format(b=boolean, year=date.today().year, laseyear=date.today().year - 1, days=ds, cmp_char=cmp,
                       now_days=365 + date.today().toordinal())
        cursor.execute(sql)
        result = cursor.fetchall()

        t_file = open("./day_{days}_state_{casestate}.json".format(days=ds, casestate=cs), 'w')

    except BaseException as inst:
        print(inst)
    else:
        data = []
        # t_file.write("\n".join(map(lambda x: "\t".join(map(str, x)), result)))
        for x in result:
            data.append([x[0].strftime('%B %d,%Y'), x[1]])
        data.reverse()
        data_str = json.dumps(data)
        t_file.write(data_str)
        t_file.flush()
        t_file.close()
        print("{now} state_{state} Query Successfully!".format(now=ft("%Y-%m-%d %H:%M:%S", lt(time())), state=cs))


# 关闭数据库
def db_close(db):
    try:
        db.close()
    except BaseException as inst:
        print(inst)
    else:
        print("{now} Database Closed!".format(now=ft("%Y-%m-%d %H:%M:%S", lt(time()))))


def main():
    db = db_connect()
    case_state_list = list(range(5, 8))

    while True:
        print()
        db_query(db, 7, 6) 
        for j in case_state_list:
            db_query(db, 120, j)
        sleep(10)
    db_close(db)


if __name__ == '__main__':
    main()
