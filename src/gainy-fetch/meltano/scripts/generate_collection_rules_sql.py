import csv
import re

queries = []
with open('../data/collections.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    for row in reader:
        name, sql, enabled = row['name'], row['sql_request'], row['enabled']
        if(not sql or not int(enabled)):
            continue
        sql = " ".join(filter(lambda x: not re.match(r'^(select|where|order)', x), sql.lower().split("\n")))
        queries.append("select collections.id as collection_id, ct.ticker_code as symbol from ct join collections on collections.name = '%s' where %s" % (name, sql))

final_sql = "\nUNION\n".join(queries)

with open('./ticker_collections.template.sql', mode='r') as template_file:
    template = template_file.read()

model = template.replace('-- __SELECT__ --', final_sql)

with open('../transform/models/ticker_collections.sql', mode='w') as model_file:
    model_file.write(model)