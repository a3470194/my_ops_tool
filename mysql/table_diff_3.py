import sys
import mysql.connector
"""
对比两个表的差异, 生成差异SQL
"""


class mysqlConn(object):
    '''    MySQL    '''
    connected = False
    conn = None
    cursor = ''
    # def mysql_conn(config, sql):
    # 构造函数，初始化时直接连接数据库
    def __init__(self, config):
        try:
            self.conn = mysql.connector.connect(**config)
            self.cursor = self.conn.cursor(dictionary=True)
        except mysql.connector.Error as e:
            print('数据库连接失败: {}\n{}-{}-{}\n'.format(e, config['host'],config['port'],config['user']), end='')
            sys.exit(0)

    def query(self, sql):
        """  Execute SQL statement """
        try:
            cur = self.cursor
            cur.execute(sql)
            data = self.cursor.fetchall()
            # print(cur.rowcount)
            # table = from_db_cursor(cur)
            # return table
            # print('ok!')
            return data
        except mysql.connector.Error as e:
            print(e)


    def __del__(self):
        """在python 进行垃圾回收时关闭连接"""
        # if self.cursor != None:
        self.cursor.close()
        # if self.conn != None:
        self.conn.close()
        # print(self.cursor)
        # print(self.conn)
        pass

#  --------  column  --------
def get_table_columns_info(config, table_name):
    db_config = config['confing']
    databases = config['db']

    query_column = """SELECT * FROM `information_schema`.`COLUMNS` 
                    WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s' 
                    ORDER BY `ORDINAL_POSITION` ASC
                    """ % (databases, table_name)

    query_column_list = mysqlConn(db_config).query(query_column)
    # print(query_column_list)
    return query_column_list


def get_column(column):
    data =  {
        'COLUMN_NAME': column['COLUMN_NAME'],
        'ORDINAL_POSITION': column['ORDINAL_POSITION'],
        'COLUMN_DEFAULT': column['COLUMN_DEFAULT'],
        'IS_NULLABLE': column['IS_NULLABLE'],
        'DATA_TYPE': column['DATA_TYPE'].decode('utf-8'),
        'CHARACTER_MAXIMUM_LENGTH': column['CHARACTER_MAXIMUM_LENGTH'],
        'CHARACTER_OCTET_LENGTH': column['CHARACTER_OCTET_LENGTH'],
        'NUMERIC_PRECISION': column['NUMERIC_PRECISION'],
        'NUMERIC_SCALE': column['NUMERIC_SCALE'],
        'DATETIME_PRECISION': column['DATETIME_PRECISION'],
        'CHARACTER_SET_NAME': column['CHARACTER_SET_NAME'],
        'COLLATION_NAME': column['COLLATION_NAME'],
        'COLUMN_TYPE': column['COLUMN_TYPE'].decode('utf-8'),
        'EXTRA': column['EXTRA'],
        'COLUMN_COMMENT': column['COLUMN_COMMENT'].decode('utf-8').replace(';',',').replace("'","")
    }
    return data

def get_column_default(column):
    # print("get_column_default  ", type(column['COLUMN_COMMENT']), column['COLUMN_COMMENT'])
    # column['COLUMN_COMMENT'] = column['COLUMN_COMMENT'].decode('utf-8')

    if column['IS_NULLABLE'] == 'NO':
        if column['COLUMN_DEFAULT'] is not None:
            if column['DATA_TYPE'] == 'timestamp' or column['DATA_TYPE'] == 'datetime':
                null_able = " NOT NULL DEFAULT %s" % column['COLUMN_DEFAULT'].decode('utf-8')
            else:
                null_able = " NOT NULL DEFAULT '%s'" % column['COLUMN_DEFAULT'].decode('utf-8')
        else:
            null_able = " NOT NULL"
    else:
        if column['COLUMN_DEFAULT'] is not None:
            if column['DATA_TYPE'] == 'timestamp' or column['DATA_TYPE'] == 'datetime':
                null_able = " NULL DEFAULT %s" % column['COLUMN_DEFAULT'].decode('utf-8')
            else:
                null_able = " DEFAULT '%s'" % column['COLUMN_DEFAULT'].decode('utf-8')
        else:
            null_able = ' DEFAULT NULL'

    # print("null_able : ",null_able)
    return null_able

def get_column_after(ordinal_position, column_pos):
    pos = ordinal_position - 1

    if pos in column_pos:
        return "AFTER `%s`" % column_pos[pos]['COLUMN_NAME']
    else:
        return "FIRST"


def reset_calc_position(column_name, local_pos, columns_online, status):
    if 1 == status:
        # ADD ...
        for k, v in columns_online.items():
            cur_pos = v['ORDINAL_POSITION']

            if cur_pos >= local_pos:
                columns_online[k]['ORDINAL_POSITION'] = columns_online[k]['ORDINAL_POSITION'] + 1
    elif 2 == status:
        # MODIFY ...
        if column_name in columns_online:
            columns_online[column_name]['ORDINAL_POSITION'] = local_pos
    elif 3 == status:
        # DROP ...
        for k, v in columns_online.items():
            cur_pos = v['ORDINAL_POSITION']

            if cur_pos >= local_pos:
                columns_online[k]['ORDINAL_POSITION'] = columns_online[k]['ORDINAL_POSITION'] - 1

    return columns_online



#  --------  schema  --------
def get_db_config(source, table_list):
    # print(source)
    user = source.split('@')[0].split(':')
    host = source.split('@')[1].split('/')[0].split(":")
    databases = source.split('@')[1].split('/')[1]
    # print(user, host, databases)

    config = {
        'user': user[0],
        'password': user[1],
        'host': host[0],
        'port': host[1],
        'database': 'information_schema',
        'charset': 'utf8',
        'autocommit': True,
        'raise_on_warnings': True
    }
    db_conf = {
        'confing':config,
        'db':databases,
        'table_list':table_list
    }
    return db_conf



def get_query_statistic(config, table_name):
    db_config = config['confing']
    databases = config['db']
    query_statistic = """SELECT * FROM `information_schema`.`STATISTICS` 
                    WHERE `TABLE_SCHEMA` = '%s' AND `TABLE_NAME` = '%s'
                    """ % (databases, table_name)
    data = mysqlConn(db_config).query(query_statistic)
    # print(data)
    return data


def get_statistic(statistic):
    return {
        'NON_UNIQUE': statistic['NON_UNIQUE'],
        'INDEX_NAME': statistic['INDEX_NAME'],
        'SEQ_IN_INDEX': statistic['SEQ_IN_INDEX'],
        'COLUMN_NAME': statistic['COLUMN_NAME'],
        'SUB_PART': statistic['SUB_PART'],
        'INDEX_TYPE': statistic['INDEX_TYPE']
    }


def get_add_keys(index_name, statistic):
    non_unique = statistic[1]['NON_UNIQUE']

    if 1 == non_unique:
        columns_name = []

        for k in sorted(statistic):
            sub_part = ''

            if statistic[k]['SUB_PART'] is not None:
                sub_part = '(%d)' % statistic[k]['SUB_PART']

            columns_name.append(
                "`{column_name}`{sub_part}".format(column_name=statistic[k]['COLUMN_NAME'], sub_part=sub_part))

        return "KEY `{index_name}` ({columns_name})".format(index_name=index_name, columns_name=",".join(columns_name))
    else:
        columns_name = []

        if 'PRIMARY' == index_name:

            for k in sorted(statistic):
                sub_part = ''

                if statistic[k]['SUB_PART'] is not None:
                    sub_part = '(%d)' % statistic[k]['SUB_PART']

                columns_name.append(
                    "`{column_name}{sub_part}`".format(column_name=statistic[k]['COLUMN_NAME'], sub_part=sub_part))

            return "PRIMARY KEY ({columns_name})".format(columns_name=",".join(columns_name))
        else:
            for k in sorted(statistic):
                sub_part = ''

                if statistic[k]['SUB_PART'] is not None:
                    sub_part = '(%d)' % statistic[k]['SUB_PART']

                columns_name.append(
                    "`{column_name}`{sub_part}".format(column_name=statistic[k]['COLUMN_NAME'], sub_part=sub_part))

            return "UNIQUE KEY `{index_name}` ({columns_name})".format(index_name=index_name,
                                                                       columns_name=",".join(columns_name))



#  --------  table  --------
def get_target_table_list(config):
    db_config = config['confing']
    databases = config['db']
    query_schema = """SELECT * FROM `information_schema`.`SCHEMATA` 
                    WHERE `SCHEMA_NAME` = '%s'
                    """ % databases
    schema_data = mysqlConn(db_config).query(query_schema)[0]
    # print(schema_data)


def get_table_info(config):
    """差异 SQL 工具。"""
    db_config = config['confing']
    databases = config['db']

    query_schema = """SELECT * FROM `information_schema`.`SCHEMATA` 
                WHERE `SCHEMA_NAME` = '%s'
                """ % databases
   
    schema_data = mysqlConn(db_config).query(query_schema)
    #print(schema_data)
    if len(schema_data) <= 0:
        # raise Exception('源数据库 `%s` 不存在。' % databases)
        print('\n-- 源数据库 `%s` 不存在 \n' % databases)
        return 'null','null'

    query_table = """SELECT * FROM `information_schema`.`TABLES` 
                WHERE `TABLE_SCHEMA` = '%s' 
                ORDER BY `TABLE_NAME` ASC
                """ % databases
    table_data_list = mysqlConn(db_config).query(query_table)
    # print(table_data_t)

    if len(table_data_list) <= 0:
        raise Exception('源数据库 `%s` 没有数据表。' % databases[0])
        #print('\n-- 源数据库 `%s` 没有数据表\n' % databases[0])

    table_data_dic = {}
    for v in table_data_list:
        table_data_dic[v['TABLE_NAME']] = v

    # print(schema_data, table_data_dic)
    return schema_data[0], table_data_dic




def get_alter_table(target_db_config, target_table_name, target_schema_data, source_db_config, source_table_name, source_schema_data):
    # print("\n\n-- 对比两张表的差异", source_table_name)
    # print(diff_sql)
    diff_sql=[]
    # 获取source的 column信息
    source_column_data_t = get_table_columns_info(source_db_config, source_table_name)
    # 获取target的 column信息
    target_column_data_t = get_table_columns_info(target_db_config, target_table_name)

    # ALTER LIST...
    alter_tables = []
    alter_columns = []
    alter_keys = []

    columns_local = {}
    columns_online = {}
    columns_pos_local = {}
    columns_pos_online = {}

    for source_column_data in source_column_data_t:
        column_data = get_column(source_column_data)
        del column_data['COLLATION_NAME']  # 不对比字段排序编码
        #print("source_column_data:",column_data)
        columns_local[source_column_data['COLUMN_NAME']] = column_data
        columns_pos_local[source_column_data['ORDINAL_POSITION']] = column_data

    for target_column_data in target_column_data_t:
        column_data = get_column(target_column_data)
        del column_data['COLLATION_NAME']  # 不对比字段排序编码
        #print("target_column_data:",column_data)
        columns_online[target_column_data['COLUMN_NAME']] = column_data
        columns_pos_online[target_column_data['ORDINAL_POSITION']] = column_data

    # 对比两个column, 是否一致
    #print("比两个column, 是否一致")
    #print(columns_pos_local)
    #print(columns_pos_online)
    if columns_pos_local != columns_pos_online:
        alter_tables.append("ALTER TABLE `%s`" % source_table_name)

        # print("# 获取远程的 column_name , 与本地数据做对比, 如果本地不存在,则删除")
        for column_name, column_online in columns_online.items():
            if column_name not in columns_local:
                columns_online = reset_calc_position(column_name, column_online['ORDINAL_POSITION'],
                                                     columns_online, 3)
                alter_columns.append("  DROP COLUMN `%s`" % column_name)

        # ADD COLUMN
        # print("# 获取本地的 column_name  与远程数据做对比, 如果远程不存在,则添加")
        for column_name, column_local in columns_local.items():
            if column_name not in columns_online:
                null_able = get_column_default(column_local)

                character = extra = column_comment = ''
                print(source_schema_data)
                print(target_schema_data)
                if column_local['CHARACTER_SET_NAME'] is not None:
                    if column_local['CHARACTER_SET_NAME'] != target_schema_data['DEFAULT_CHARACTER_SET_NAME']:
                        character = ' CHARACTER SET %s' % column_local['CHARACTER_SET_NAME']

                if column_local['EXTRA'] != '':
                    extra = ' %s' % column_local['EXTRA'].upper().replace("DEFAULT_GENERATED","")

                if column_local['COLUMN_COMMENT'] != '':
                    column_comment = f" COMMENT '{column_local['COLUMN_COMMENT']}'"

                after = get_column_after(column_local['ORDINAL_POSITION'], columns_pos_local)

                # 重新计算字段位置
                columns_online = reset_calc_position(column_name, column_local['ORDINAL_POSITION'],
                                                     columns_online, 1)

                #print("新计算字段位置", character)
                alter_columns.append(
                    "  ADD COLUMN `{column_name}` {column_type}{character}{null_able}{extra}{column_comment} {after}".format(
                        column_name=column_name, column_type=column_local['COLUMN_TYPE'], character=character,
                        null_able=null_able, extra=extra, after=after,
                        column_comment=column_comment))

        # MODIFY COLUMN
        for column_name, column_local in columns_local.items():
            if column_name in columns_online:
                #print(f"对比 {column_name}\n{column_local}\n{columns_online[column_name]}")
                if str(column_local) != str(columns_online[column_name]):
                    #print("字段不一样 ",str(column_local), str(columns_online[column_name]))
                    null_able = get_column_default(column_local)

                    character = extra = column_comment = ''

                    if column_local['CHARACTER_SET_NAME'] is not None:
                        if column_local['CHARACTER_SET_NAME'] != target_schema_data['DEFAULT_CHARACTER_SET_NAME']:
                            character = ' CHARACTER SET %s' % column_local['CHARACTER_SET_NAME']


                    print("character")
                    print(column_local)
                    print(source_schema_data)
                    print(target_schema_data)
                    if column_local['EXTRA'] != '':
                        extra = ' %s' % column_local['EXTRA'].upper().replace("DEFAULT_GENERATED","")

                    if column_local['COLUMN_COMMENT'] != '':
                        column_comment = f" COMMENT '{column_local['COLUMN_COMMENT']}'"

                    after = get_column_after(column_local['ORDINAL_POSITION'], columns_pos_local)

                    # 重新计算字段位置
                    columns_online = reset_calc_position(column_name, column_local['ORDINAL_POSITION'],
                                                           columns_online, 2)
                    #print("新计算字段位置", character)
                    alter_columns.append(
                        "  MODIFY COLUMN `{column_name}` {column_type}{character}{null_able}{extra}{column_comment} {after}".format(
                            column_name=column_name, column_type=column_local['COLUMN_TYPE'], character=character,
                            null_able=null_able,
                            extra=extra, after=after, column_comment=column_comment))

    source_statistic_data_t = get_query_statistic(source_db_config, source_table_name)
    # print("source_statistic_data_t:", source_statistic_data_t)

    target_statistic_data_t = get_query_statistic(target_db_config, target_table_name)
    # print("target_statistic_data_t:", target_statistic_data_t)

    source_statistic_data_count = len(source_statistic_data_t)

    if source_statistic_data_count > 0:
        statistics_local = {}
        statistics_online = {}

        for source_statistic_data in source_statistic_data_t:
            if source_statistic_data['INDEX_NAME'] in statistics_local:
                statistics_local[source_statistic_data['INDEX_NAME']].update({
                    source_statistic_data['SEQ_IN_INDEX']: get_statistic(source_statistic_data)
                })
            else:
                statistics_local[source_statistic_data['INDEX_NAME']] = {
                    source_statistic_data['SEQ_IN_INDEX']: get_statistic(source_statistic_data)
                }

        for target_statistic_data in target_statistic_data_t:
            if target_statistic_data['INDEX_NAME'] in statistics_online:
                statistics_online[target_statistic_data['INDEX_NAME']].update({
                    target_statistic_data['SEQ_IN_INDEX']: get_statistic(target_statistic_data)
                })
            else:
                statistics_online[target_statistic_data['INDEX_NAME']] = {
                    target_statistic_data['SEQ_IN_INDEX']: get_statistic(target_statistic_data)
                }

        if statistics_local != statistics_online:
            if not alter_tables:
                alter_tables.append("ALTER TABLE `%s`" % source_table_name)

            for index_name, statistic_online in statistics_online.items():
                if index_name not in statistics_local:
                    if 'PRIMARY' == index_name:
                        alter_keys.append("  DROP PRIMARY KEY")
                    else:
                        alter_keys.append("  DROP INDEX `%s`" % index_name)

            for index_name, statistic_local in statistics_local.items():
                if index_name in statistics_online:
                    # DROP INDEX ... AND ADD KEY ...
                    if statistic_local != statistics_online[index_name]:
                        if 'PRIMARY' == index_name:
                            alter_keys.append("  DROP PRIMARY KEY")
                        else:
                            alter_keys.append("  DROP INDEX `%s`" % index_name)

                        alter_keys.append("  ADD %s" % get_add_keys(index_name, statistic_local))
                else:
                    # ADD KEY
                    alter_keys.append("  ADD %s" % get_add_keys(index_name, statistic_local))

            if alter_keys:
                for alter_key in alter_keys:
                    alter_columns.append(alter_key)

    if alter_columns:
        for alter_column in alter_columns:
            if alter_column == alter_columns[-1]:
                column_dot = ';'
            else:
                column_dot = ','

            alter_tables.append('%s%s' % (alter_column, column_dot))

    if alter_tables:
        diff_sql.append('\n'.join(alter_tables))
    else:
        print("-- 两张表没有差异!")

    return diff_sql



def get_create_table(source_schema_data, source_db_config, source_table_name,source_table_data):
    print(f"对比表{source_table_name}")
    diff_sql = []
    source_statistics_data_t = get_query_statistic(source_db_config, source_table_name)
    source_statistics_data_count = len(source_statistics_data_t)

    create_tables = ["CREATE TABLE IF NOT EXISTS `%s` (" % source_table_name]
    # COLUMN...
    source_column_data_t = get_table_columns_info(source_db_config, source_table_name)
    # print("source_column_data_t:",source_column_data_t)
    for column_data in source_column_data_t:
        # print(f"目标表{source_table_name}在源数据库中 不存在 ", type(source_column_data_t))

        column = get_column(column_data)
        null_able = get_column_default(column)
        character = extra = dot = comment = ''
        if column['CHARACTER_SET_NAME'] is not None:
            if column['CHARACTER_SET_NAME'] != source_schema_data['DEFAULT_CHARACTER_SET_NAME']:
                character = ' CHARACTER SET %s' % column['CHARACTER_SET_NAME']

        if column['EXTRA'] != '':
            extra = ' %s' % column['EXTRA'].upper().replace("DEFAULT_GENERATED","")

        if column['COLUMN_COMMENT'] != '':
            comment = f" COMMENT '{column['COLUMN_COMMENT']}'"

        if column != source_column_data_t[-1] or source_statistics_data_count > 0:
            dot = ','


        create_tables.append(
            "  `{column_name}` {column_type}{character}{null_able}{extra}{column_comment}{dot}".format(
                column_name=column['COLUMN_NAME'],
                column_type=column['COLUMN_TYPE'],
                character=character,
                null_able=null_able,
                extra=extra,
                dot=dot,
                column_comment=comment)
        )

    # KEY...

    create_tables_keys = []
    table_comment = ''
    if source_statistics_data_count > 0:
        source_statistics_data_dic = {}

        for source_statistics_data in source_statistics_data_t:
            if source_statistics_data['INDEX_NAME'] in source_statistics_data_dic:
                source_statistics_data_dic[source_statistics_data['INDEX_NAME']].update({
                    source_statistics_data['SEQ_IN_INDEX']: source_statistics_data
                })
            else:
                source_statistics_data_dic[source_statistics_data['INDEX_NAME']] = {
                    source_statistics_data['SEQ_IN_INDEX']: source_statistics_data
                }

        for index_name, source_statistics_data in source_statistics_data_dic.items():
            create_tables_keys.append(
                "  {key_slot}".format(key_slot=get_add_keys(index_name, source_statistics_data)))

    if source_table_data['TABLE_COMMENT'] != '':
        table_comment = f" COMMENT='{source_table_data['TABLE_COMMENT']}'"
    create_tables.append(",\n".join(create_tables_keys))
    create_tables.append(
        ") ENGINE={engine} DEFAULT CHARSET={charset}{comment};".format(engine=source_table_data['ENGINE'],
                                                            charset=source_schema_data['DEFAULT_CHARACTER_SET_NAME'],
                                                            comment=table_comment))

    diff_sql.append("\n".join(create_tables))
    return diff_sql


def check_table(source_db_config, target_db_config,):
    if source_db_config['table_list'] is None or target_db_config['table_list'] is None:
        print("-- 全库对比")
        source_schema_data, source_table_data_dic = get_table_info(source_db_config)
        if source_schema_data == "null" or source_table_data_dic == "null":
            #print(f"-- 库不存在\n\n")
            return

        # print(f"use {source_schema_data['SCHEMA_NAME']};")
        source_table_list = list(source_table_data_dic.keys())

        target_schema_data, target_table_data_dic = get_table_info(target_db_config)
        target_table_list = list(target_table_data_dic.keys())
        # print(source_table_list)
        # print(target_table_list)
        # print(f"{source_schema_data}\n{target_schema_data}")
        for source_table_name in source_table_list:
            print(f'\n--  -------------')
            print(f'-- 检查表 source: {source_table_name} , target: {source_table_name}--\n')
            source_table_data = source_table_data_dic[source_table_name]

            if source_table_name not in source_table_list:
                raise Exception(f'表{source_table_name}在源数据库 {source_db_config["db"]} 中 不存在')

            # 把source_table_name放到target_table_list中做对比, 如果存在,就对比字段, 否则创建表
            if source_table_name in target_table_list:
                # ALTER TABLE
                # print(f"-- 表{source_table_name}在目标库 {target_db_config['db']} 中 存在 ")
                diff_sql = get_alter_table(target_db_config, source_table_name, target_schema_data,
                                          source_db_config, source_table_name, source_schema_data)

            else:
                # CREATE TABLE...
                # print(f"-- 表{source_table_name}在目标库 {target_db_config['db']} 中 不存在 ")
                diff_sql = get_create_table(source_schema_data, source_db_config, source_table_name,source_table_data)

            if diff_sql:
                # print('\nSET NAMES %s;\n' % source_schema_data['DEFAULT_CHARACTER_SET_NAME'])
                print("\n\n".join(diff_sql))

    else:
        print("-- 指定表对比")
        source_table_name_list = source_db_config['table_list']
        target_table_name_list = target_db_config['table_list']
        for source_table_name, target_table_name in zip(source_table_name_list, target_table_name_list):
            print(f'\n\n--  -------------')
            print(f'-- 检查表 source: {source_table_name} , target: {target_table_name}--\n')

            source_schema_data, source_table_data_dic = get_table_info(source_db_config)
            if source_schema_data == "null" or source_table_data_dic == "null":
                #print(f"-- 库不存在\n\n")
                return

            source_table_list = list(source_table_data_dic.keys())
            # print(source_table_name, source_table_list)
            if source_table_name not in source_table_list:
                print(f"-- {source_table_name} 此表不存在")
                continue

            target_schema_data, target_table_data_dic = get_table_info(target_db_config)
            target_table_list = list(target_table_data_dic.keys())

            source_table_data = source_table_data_dic[source_table_name]

            if source_table_name not in source_table_list:
                raise Exception(f'表{source_table_name}在源数据库 {source_db_config["db"]} 中 不存在')

            # 把source_table_name放到target_table_list中做对比, 如果存在,就对比字段, 否则创建表
            if source_table_name in target_table_list:
                # ALTER TABLE
                # print(f"-- 表{source_table_name}在目标库 {target_db_config['db']} 中 存在 ")
                diff_sql = get_alter_table(target_db_config, target_table_name, target_schema_data,
                                           source_db_config, source_table_name, source_schema_data)

            else:
                # CREATE TABLE...
                # print(f"-- 表{source_table_name}在目标库 {target_db_config['db']} 中 不存在 ")
                diff_sql = get_create_table(source_schema_data, source_db_config, source_table_name, source_table_data)

            if diff_sql:
                # print('\nSET NAMES %s;\n' % source_schema_data['DEFAULT_CHARACTER_SET_NAME'])
                print("\n\n".join(diff_sql))

"""
MySQL8.0 设置简单密码
SHOW VARIABLES LIKE 'validate_password%';
set global validate_password_policy=0;
set global validate_password_length=1;
CREATE USER IF NOT EXISTS 'testdba'@'%' IDENTIFIED WITH mysql_native_password BY 'testdba';
GRANT ALL PRIVILEGES ON *.* TO 'testdba'@'%';
flush privileges;
"""

if __name__ == '__main__':
    db_list=["lepu_access_control", "lepu_activity_center", "lepu_aftersale", "lepu_baseserver", "lepu_ca", "lepu_cart_center", "lepu_consultation_user_support", "lepu_coupon", "lepu_dict", "lepu_download", "lepu_drug", "lepu_drugstore_settings", "lepu_grc", "lepu_hospital", "lepu_im", "lepu_job", "lepu_kit", "lepu_media_center", "lepu_member_center", "lepu_merchant_center", "lepu_monitor", "lepu_mq_manager", "lepu_order_center", "lepu_pay", "lepu_product_center", "lepu_push", "lepu_qrcode_center", "lepu_qrcode_center_validate", "lepu_register", "lepu_resource", "lepu_setting_center", "lepu_settlement_center", "lepu_statistics", "lepu_store_center", "lepu_tech_center", "lepu_trade", "lepu_treat", "lepu_user_info", "lepu_verification"]
    #db_list=["lepu_user_info"]
    for i in db_list:
        print(f"\n-- ++++ dbname : {i} ++++ --")
        source = f'testdba:testdba@192.168.0.1:3307/{i}'  # user:password@IP:port/dbname
        target = f'testdba:testdba@192.168.0.1:3308/{i}'  # user:password@IP:port/dbname
        source_table_name_list = None    # 全库查询就用None
        target_table_name_list = None    # 全库查询就用None
        # ----指定对比的表名, 两个列表上下一一对应, 第一列和第一列的表对比, 第二列和第二列的表对比,依此类推----
        #source_table_name_list = ["mc_member_info_bak", "test1", "dept", "user"]   # 指定要查询的表
        #target_table_name_list = ["mc_member_info_bak", "test1", "dept", "dept"]   # 指定要查询的表

        source_db_config = get_db_config(source, source_table_name_list)
        target_db_config = get_db_config(target, target_table_name_list)
        check_table(source_db_config, target_db_config)
