import sqlalchemy
from google.cloud import storage
from flask import jsonify

connection_name = "sep6-314214:us-central1:mysep6sql"
db_name = "movies_db"
db_user = "root"
db_password = "3223"

driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

def get_last20Years(request):
  db = sqlalchemy.create_engine(
    sqlalchemy.engine.url.URL(
    drivername=driver_name,
    username=db_user,
    password=db_password,
    database=db_name,
    query=query_string,
    ),
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
    pool_recycle=1800
  )
  result = []
  years = range(2000, 2022)
  for year in years:       
      query = "SELECT COUNT(*) FROM movies WHERE year = " + f'{year}'
      with db.engine.begin() as conn:
        result.append(conn.execute(query).fetchone())

  result = [i[0] for i in result]
  data = []
  for i in range(0,len(years)):
        data.append({"year" : years[i],"movies" : result[i]})     
  jsonStr = {'result' : data}
  return jsonify(jsonStr)