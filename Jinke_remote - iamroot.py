# -*- coding: utf8 -*-
# 必要扩展包
import json
import pymysql as pms
from time import strftime as ft, time, localtime as lt, sleep
from datetime import date, timedelta

one_day = []


# 连接到数据库 主机名/用户名/密码/数据库/字符集
def db_connect():
    try:
        # pmt = ["rm-8vb78ejw3050i8u870o.mysql.zhangbei.rds.aliyuncs.com", "jincomed", "Jinke@2017", "h1", "utf8"]
        pmt = ["localhost", "Jinke", "000000", "project", "utf8"]
        db = pms.connect(host=pmt[0], user=pmt[1], passwd=pmt[2], db=pmt[3], charset=pmt[4])
    except BaseException as inst:
        print(inst)
    else:
        print("{} Database Connected!".format(ft("%Y-%m-%d %H:%M:%S", lt(time()))))
        return db


# 数据库查询 db 数据库名/ ds 查询天数/ cs 案例类型
def db_query(db, ds, cs):
    global one_day
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
        (
            SELECT
                DATE(createtime) create_time,
                COUNT(*) count
            FROM
                cases{year}
            WHERE
                hospitalid = 'H10000107'
            AND casestate {cmp_char} 6
            AND {now_days} - TO_DAYS(createtime) <= {days} - 1
            GROUP BY
                create_time
        )
        UNION
        (
            SELECT
                DATE(createtime) create_time,
                COUNT(*) count
            FROM
                cases{laseyear}
            WHERE
                hospitalid = 'H10000107'
            AND casestate {cmp_char} 6
            AND {now_days} - TO_DAYS(createtime) <= {days} - 1
            GROUP BY
                create_time
        )
        ORDER BY
            create_time
            '''.format(year=date.today().year, laseyear=date.today().year - 1, days=ds,
                       now_days=365 + date.today().toordinal(), cmp_char=cmp)
        cursor.execute(sql)
        result = cursor.fetchall()

        t_file = open("./day_{days}_state_{casestate}.json".format(days=ds, casestate=cs), 'w')
        t_file_7 = open("./day_7_state_{casestate}.json".format(casestate=cs), 'w')
        t_file_3 = open("./day_3_state_{casestate}.json".format(casestate=cs), 'w')

    except BaseException as inst:
        print(inst)
    else:
        data = []
        # t_file.write("\n".join(map(lambda x: "\t".join(map(str, x)), result)))
        pos = 0
        length = len(result)
        for i in range(ds - 1, -1, -1):
            if pos >= length - 1 or result[pos][0] != date.today() - timedelta(i):
                data.append([(date.today() - timedelta(i)).strftime('%B %d,%Y'), 0])
            else:
                data.append([result[pos][0].strftime('%B %d,%Y'), result[pos][1]])
                pos += 1
        # data.reverse()
        data_str = json.dumps(data)
        t_file.write(data_str)
        t_file.flush()
        t_file.close()

        l = len(data)
        data_str = json.dumps(data[l - 7:l])
        t_file_7.write(data_str)
        t_file_7.flush()
        t_file_7.close()

        data_str = json.dumps(data[l - 3:l])
        t_file_3.write(data_str)
        t_file_3.flush()
        t_file_3.close()

        one_day.append(data[l - 1][1])
        print("{now} state_{state} Query Successfully!".format(now=ft("%Y-%m-%d %H:%M:%S", lt(time())), state=cs))


def db_count(db):
    global one_day
    sql = \
        '''
    (
        SELECT
            COUNT(*)
        FROM
            cases{lastyear}
    )
    UNION
    (
        SELECT
            COUNT(*)
        FROM
            cases{year}
    )
        '''.    format(year=date.today().year, lastyear=date.today().year - 1)
    cursor = db.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    all_total = result[0][0] + result[1][0]

    sql = \
        '''
    (
        SELECT
            COUNT(*)
        FROM
            cases{lastyear}
        WHERE
            hospitalid = 'H10000107'
    )
    UNION
    (
        SELECT
            COUNT(*)
        FROM
            cases{year}
        WHERE
            hospitalid = 'H10000107'
    )
        '''.format(year=date.today().year, lastyear=date.today().year - 1)
    cursor = db.cursor()
    cursor.execute(sql)
    result = cursor.fetchall()
    jinke_total = result[0][0] + result[1][0]

    t_file = open("./total.json", 'w')
    t_file.write(str([all_total, jinke_total]))
    t_file.flush()
    t_file.close()

    t_file_1 = open("./day_1.json", "w")
    t_file_1.write(json.dumps(one_day))
    t_file_1.flush()
    t_file_1.close()


# 关闭数据库
def db_close(db):
    try:
        db.close()
    except BaseException as inst:
        print(inst)
    else:
        print("{now} Database Closed!".format(now=ft("%Y-%m-%d %H:%M:%S", lt(time()))))


def main():
    global one_day
    db = db_connect()
    case_state_list = list(range(5, 8))
    while True:
        one_day=[]
        for i in case_state_list:
            db_query(db, 120, i)
        db_count(db)
        sleep(10)
    # db_close(db)


if __name__ == '__main__':
    main()
