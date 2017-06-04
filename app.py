#!/usr/bin/env python3

import psycopg2

instruction1 = 'Top 3 most viewed articles:'

query1 = '''SELECT articles.title, COUNT(log.path) FROM articles, log WHERE
log.path = concat('/article/', articles.slug) GROUP BY articles.title ORDER
BY count DESC LIMIT 3;'''

instruction2 = 'Author rankings based on views:'

query2 = '''SELECT authors.name, Count(log.path) FROM log, authors
left join articles ON authors.id = articles.author
WHERE log.path = concat('/article/', articles.slug)
GROUP BY authors.name ORDER BY count DESC'''

instruction3 = 'Days where more than 1% of requests lead to errors:'

query3 = '''SELECT TO_CHAR(date, 'FMMONTH FMDD, YYYY') AS date, (error/count)
AS percent FROM (SELECT TIME::DATE AS date, COUNT(*) AS count,
SUM((status != '200 OK')::int::float) AS error FROM log GROUP BY date)
AS errors WHERE (error/count) > 0.01 ORDER BY percent DESC;'''

queries = [query1, query2, query3]

instructions = [instruction1, instruction2, instruction3]

units = ['views', 'views', 'errors']


def connect(database_name="news"):
    try:
        db = psycopg2.connect("dbname={}".format(database_name))
        c = db.cursor()
        return db, c
    except:
        print("<error message>")


def runQuery():
    db, c = connect()
    for i in range(0, 3):
        c.execute(queries[i])
        sortedData = c.fetchall()
        print(instructions[i])
        for z in range(len(sortedData)):
            leftCol = sortedData[z][0]
            rightCol = sortedData[z][1]
            if i == 2:
                rightCol = "{:.1%}".format(rightCol)
            print('%s -- %s %s' % (leftCol, rightCol, units[i]))
    db.commit()
    db.close()


runQuery()
