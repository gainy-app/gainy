import csv
import re


def generate_dbt_model(model_name: str, source_name: str, sql_generator):
    queries = []
    with open('../data/%s.csv' % (source_name)) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            name, sql, enabled = row['name'], row['sql_request'], row[
                'enabled']
            if (not sql or not int(enabled)):
                continue

            lines = sql.split("\n")
            lines = filter(lambda x: not re.match(r'^(select|order)', x),
                           lines)
            lines = map(lambda x: x.replace('where', '').strip(), lines)
            sql = " ".join(lines)
            name = name.replace("'", "''")

            queries.append(sql_generator(name, sql))

    final_sql = "\nUNION\n".join(queries)

    with open('./templates/%s.template.sql' % (model_name),
              mode='r') as template_file:
        template = template_file.read()

    model = template.replace('-- __SELECT__ --', final_sql)

    with open('../transform/models/%s.sql' % (model_name),
              mode='w') as model_file:
        model_file.write(model)


generate_dbt_model(
    "ticker_collections", "collections", lambda name, sql:
    "select collections.id as collection_id, ct.ticker_code as symbol from ct join {{ ref('collections') }} on collections.name = '%s' where %s"
    % (name, sql))

generate_dbt_model(
    "interest_industries", "interests", lambda name, sql:
    "select interests.id as interest_id, ct.g_industry_id as industry_id from ct join {{ ref ('interests') }} on interests.name = '%s' where %s"
    % (name, sql))
