import psycopg2

try:
    db = psycopg2.connect("dbname='...' user='...' host='localhost' password='...'")
    print(db)
except ConnectionError as e:
    print("failed", e)
    exit(1)

exit(0)
