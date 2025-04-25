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


def batch_insert(cursor, cnx, query, data_list, table_name):
    try:
        cursor.executemany(query, data_list)
        cnx.commit()
        return len(data_list), 0
    except mysql.connector.Error as err:
        cnx.rollback()
        # 如果批量失败，改为逐条插入
        success = 0
        fail = 0
        for data in data_list:
            try:
                cursor.execute(query, data)
                cnx.commit()
                success += 1
            except mysql.connector.Error as err:
                cnx.rollback()
                fail += 1
                print(f"{table_name} 插入失败 (错误: {err})")
        return success, fail


def process_json_to_mysql(file_path, max_records=100000, batch_size=1000):
    cnx = get_db_connection()
    cursor = cnx.cursor()

    # 初始化计数器
    record_count = 0
    success_counts = {
        'object_t': 0,
        'object_attribute_t': 0,
        'object_format': 0,
        'object_sense_t': 0,
        'object_claim_t': 0
    }
    fail_counts = {
        'object_t': 0,
        'object_attribute_t': 0,
        'object_format': 0,
        'object_sense_t': 0,
        'object_claim_t': 0
    }

    # 开始计时
    start_time = time.time()
    last_print_time = start_time

    with bz2.open(file_path, 'rb') as f:
        for i, line in enumerate(f):
            try:
                line_str = line.decode('utf-8').strip()
                if line_str.endswith(','):
                    line_str = line_str[:-1]
                if line_str:
                    data = json.loads(line_str)
                    object_id = data.get('id')

                    # 初始化批量数据容器
                    batch_objects = []
                    batch_attributes = []
                    batch_forms = []
                    batch_senses = []
                    batch_claims = []

                    # 1. 准备object_t表数据
                    lemmas = data.get('lemmas', {}).get('en', {}) if data.get('lemmas') else {}
                    batch_objects.append((
                        object_id,
                        data.get('type'),
                        'en',
                        lemmas.get('value')
                    ))

                    # 2. 准备object_attribute_t表数据
                    batch_attributes.append((
                        object_id,
                        data.get('lexicalCategory'),
                        data.get('language'),
                        str(data.get('lastrevid')) if data.get('lastrevid') else None
                    ))

                    # 3. 准备object_format表数据
                    forms = data.get('forms', [])
                    for form in forms:
                        batch_forms.append((
                            object_id,
                            form.get('id'),
                            'en',
                            form.get('representations', {}).get('en', {}).get('value'),
                            json.dumps(form.get('grammaticalFeatures'), ensure_ascii=False) if form.get(
                                'grammaticalFeatures') else None
                        ))

                    # 4. 准备object_sense_t表数据
                    senses = data.get('senses', [])
                    for sense in senses:
                        batch_senses.append((
                            object_id,
                            sense.get('id'),
                            'en',
                            json.dumps(sense.get('glosses', {}).get('en', {}).get('value'),
                                       ensure_ascii=False) if sense.get('glosses') else None
                        ))

                    # 5. 准备object_claim_t表数据
                    claims = data.get('claims', {})
                    for prop, claim_list in claims.items():
                        for claim in claim_list:
                            batch_claims.append((
                                object_id,
                                claim.get('id'),
                                json.dumps(claim, ensure_ascii=False)
                            ))

                    # 批量插入数据
                    if len(batch_objects) > 0:
                        success, fail = batch_insert(cursor, cnx,
                                                     "INSERT INTO object_t (id, type, lemmas_lan, lemmas_value) VALUES (%s, %s, %s, %s)",
                                                     batch_objects, "object_t")
                        success_counts['object_t'] += success
                        fail_counts['object_t'] += fail

                    if len(batch_attributes) > 0:
                        success, fail = batch_insert(cursor, cnx,
                                                     "INSERT INTO object_attribute_t (id, category, language, lastrevid) VALUES (%s, %s, %s, %s)",
                                                     batch_attributes, "object_attribute_t")
                        success_counts['object_attribute_t'] += success
                        fail_counts['object_attribute_t'] += fail

                    if len(batch_forms) > 0:
                        success, fail = batch_insert(cursor, cnx,
                                                     "INSERT INTO object_format (id, form_id, rep_lan, rep_value, gramma) VALUES (%s, %s, %s, %s, %s)",
                                                     batch_forms, "object_format")
                        success_counts['object_format'] += success
                        fail_counts['object_format'] += fail

                    if len(batch_senses) > 0:
                        success, fail = batch_insert(cursor, cnx,
                                                     "INSERT INTO object_sense_t (id, sense_id, sense_lan, sense_value) VALUES (%s, %s, %s, %s)",
                                                     batch_senses, "object_sense_t")
                        success_counts['object_sense_t'] += success
                        fail_counts['object_sense_t'] += fail

                    if len(batch_claims) > 0:
                        success, fail = batch_insert(cursor, cnx,
                                                     "INSERT INTO object_claim_t (id, claim_id, claim_text) VALUES (%s, %s, %s)",
                                                     batch_claims, "object_claim_t")
                        success_counts['object_claim_t'] += success
                        fail_counts['object_claim_t'] += fail

                    record_count += 1

                    # 每处理batch_size条记录打印一次进度
                    if record_count % batch_size == 0:
                        current_time = time.time()
                        elapsed = current_time - last_print_time
                        last_print_time = current_time
                        print(f"批量处理进度: 已处理 {record_count}/{max_records} 条 [本轮耗时: {elapsed:.2f}秒]")

                    # 达到最大记录数时停止
                    if record_count >= max_records:
                        break

            except json.JSONDecodeError as e:
                print(f"解析错误（行 {i + 1}）：{e}")
                continue

    # 计算耗时
    elapsed_time = time.time() - start_time

    print(f"\n处理完成！")
    print(f"总处理记录数: {record_count} (目标: {max_records})")
    print(f"总耗时: {elapsed_time:.2f} 秒")
    print(f"平均速度: {record_count / elapsed_time:.2f} 条/秒")
    print("\n各表插入统计:")
    for table in success_counts:
        print(f"{table}: 成功 {success_counts[table]}, 失败 {fail_counts[table]}")

    cursor.close()
    cnx.close()


# 执行导入
file_path = r'E:\python_projects\Intelligent_data\latest-lexemes.json.bz2'
process_json_to_mysql(file_path, max_records=100000, batch_size=1000)