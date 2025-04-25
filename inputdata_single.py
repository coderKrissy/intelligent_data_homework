import bz2
import json
import mysql.connector
from mysql.connector import errorcode
import time

# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'wikidata',
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        conn.set_charset_collation('utf8mb4')
        return conn
    except mysql.connector.Error as err:
        print(f"数据库连接错误: {err}")
        exit(1)

def process_json_to_mysql(file_path, batch_size=1000, max_records=100000):
    cnx = get_db_connection()
    cursor = cnx.cursor()

    # 准备插入语句
    insert_query = """
    INSERT INTO json_t (
        id, type, lemmas, lexical_category, language, 
        claims, forms, senses, lastrevid
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # 初始化计数器
    total_count = 0
    success_count = 0
    fail_count = 0
    batch_count = 0
    batch_records = []

    # 开始计时
    start_time = time.time()

    with bz2.open(file_path, 'rb') as f:
        for i, line in enumerate(f):
            try:
                line_str = line.decode('utf-8').strip()
                if line_str.endswith(','):
                    line_str = line_str[:-1]
                if line_str:
                    data = json.loads(line_str)

                    # 准备数据
                    record = (
                        data.get('id'),
                        data.get('type'),
                        json.dumps(data.get('lemmas'), ensure_ascii=False) if data.get('lemmas') else None,
                        data.get('lexicalCategory'),
                        data.get('language'),
                        json.dumps(data.get('claims'), ensure_ascii=False) if data.get('claims') else None,
                        json.dumps(data.get('forms'), ensure_ascii=False) if data.get('forms') else None,
                        json.dumps(data.get('senses'), ensure_ascii=False) if data.get('senses') else None,
                        str(data.get('lastrevid')) if data.get('lastrevid') else None
                    )

                    batch_records.append(record)
                    total_count += 1
                    batch_count += 1

                    # 达到批处理大小时执行批量插入
                    if batch_count >= batch_size:
                        try:
                            cursor.executemany(insert_query, batch_records)
                            cnx.commit()
                            success_count += batch_count
                            print(f"批量插入成功: 本次处理 {batch_count} 条，累计成功 {success_count} 条")
                        except mysql.connector.Error as err:
                            cnx.rollback()
                            fail_count += batch_count
                            print(f"批量插入失败 (错误: {err})，跳过 {batch_count} 条记录")

                        # 重置批处理
                        batch_records = []
                        batch_count = 0

                    # 达到最大记录数时停止
                    if total_count >= max_records:
                        break

            except json.JSONDecodeError as e:
                print(f"解析错误（行 {i + 1}）：{e}")
                continue

    # 处理最后一批不足batch_size的记录
    if batch_records:
        try:
            cursor.executemany(insert_query, batch_records)
            cnx.commit()
            success_count += len(batch_records)
            print(f"最后一批插入成功: 本次处理 {len(batch_records)} 条，累计成功 {success_count} 条")
        except mysql.connector.Error as err:
            cnx.rollback()
            fail_count += len(batch_records)
            print(f"最后一批插入失败 (错误: {err})，跳过 {len(batch_records)} 条记录")

    # 计算耗时
    elapsed_time = time.time() - start_time

    print(f"\n处理完成！")
    print(f"总处理记录数: {total_count}")
    print(f"成功插入: {success_count}")
    print(f"插入失败: {fail_count}")
    print(f"总耗时: {elapsed_time:.2f} 秒")
    print(f"平均速度: {total_count / elapsed_time:.2f} 条/秒")

    cursor.close()
    cnx.close()

# 执行导入
file_path = r'E:\python_projects\Intelligent_data\latest-lexemes.json.bz2'
process_json_to_mysql(file_path, batch_size=1000, max_records=100000)