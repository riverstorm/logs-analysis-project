#!/usr/bin/env python3
import psycopg2

conn = psycopg2.connect("dbname=news")
cursor = conn.cursor()
# Select the top 3 articles by count of impressions from logs
cursor.execute(
    "SELECT articles.title, count(log.path) "
    "FROM articles "
    "LEFT JOIN log ON log.path LIKE '%' || articles.slug || '%' "
    "GROUP BY articles.title "
    "ORDER BY count(log.path) DESC "
    "LIMIT 3 "
)
articles = cursor.fetchall()
# Select the top 3 authors by count of impressions from logs
cursor.execute(
    "SELECT authors.name, count(log.path) "
    "FROM authors "
    "LEFT JOIN articles ON articles.author=authors.id "
    "LEFT JOIN log ON log.path LIKE '%' || articles.slug || '%' "
    "GROUP BY authors.name "
    "ORDER BY count(log.path) DESC "
    "LIMIT 4 "
)
authors = cursor.fetchall()
# Select all days that exceeded a 1% error rate from logs
cursor.execute(
    "SELECT date(time), "
    "(SUM(CASE status WHEN '200 OK' THEN 0 ELSE 1 END) * 100.0 "
    "/ COUNT(status)) "
    "FROM log "
    "GROUP BY date(time) "
    "HAVING (SUM(CASE status WHEN '200 OK' THEN 0 ELSE 1 END) * 100.0 "
    "/ COUNT(status)) >= 1 "
    "ORDER BY date(time) "
)
errors = cursor.fetchall()
conn.close()

# Write to file
output_file = open('statistics.txt', 'w')

output_file.write(
    "What are the most popular three articles of all time? \r\n")
for val in articles:
    output_file.write(val[0] + " - " + str(val[1]) + " impressions \r\n")

output_file.write(
    "\r\nWho are the most popular article authors of all time? \r\n")
for val in authors:
    output_file.write(val[0] + " - " + str(val[1]) + " impressions \r\n")

output_file.write(
    "\r\nOn which days did more than 1% of requests lead to errors? \r\n")
for val in errors:
    errors_date = val[0].strftime('%m/%d/%Y')
    errors_percentage = round(val[1], 2)
    errors_percentage = str(errors_percentage)
    output_file.write(
        errors_date + " - " + errors_percentage + "% error rate \r\n")

output_file.close()
