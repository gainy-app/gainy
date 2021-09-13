import csv
import re

queries = []
with open('../data/collections.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    for row in reader:
        name, sql, enabled = row['name'], row['sql_request'], row['enabled']
        if(not sql or not int(enabled)):
            continue

        lines = sql.lower().split("\n")
        lines = filter(lambda x: not re.match(r'^(select|order)', x), lines)
        lines = map(lambda x: x.replace('where', '').strip(), lines)
        sql = " ".join(lines)
        queries.append("select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '%s' where %s" % (name, sql))

final_sql = "\nUNION\n".join(queries)

with open('./ticker_collections.template.sql', mode='r') as template_file:
    template = template_file.read()

model = template.replace('-- __SELECT__ --', final_sql)

with open('../transform/models/ticker_collections.sql', mode='w') as model_file:
    model_file.write(model)