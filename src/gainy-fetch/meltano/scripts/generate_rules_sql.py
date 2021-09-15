import csv
import re

models = [
    'interests',
    'collections'
]

for model_name in models:
    model_name_singular = re.sub(r's$', '', model_name)
    queries = []
    with open('../data/%s.csv' % (model_name)) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            name, sql, enabled = row['name'], row['sql_request'], row['enabled']
            if(not sql or not int(enabled)):
                continue

            lines = sql.lower().split("\n")
            lines = filter(lambda x: not re.match(r'^(select|order)', x), lines)
            lines = map(lambda x: x.replace('where', '').strip(), lines)
            sql = " ".join(lines)

            queries.append(
                "select %s.id as %s_id, ct.ticker_code as symbol from ct join %s on %s.name = '%s' where %s" %
                (model_name, model_name_singular, model_name, model_name, name, sql)
            )

    final_sql = "\nUNION\n".join(queries)

    with open('./templates/ticker_%s.template.sql' % (model_name), mode='r') as template_file:
        template = template_file.read()

    model = template.replace('-- __SELECT__ --', final_sql)

    with open('../transform/models/ticker_%s.sql' % (model_name), mode='w') as model_file:
        model_file.write(model)