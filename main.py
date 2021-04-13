from flask import Flask, render_template, flash, request
import sqlite3, csv
import pandas as pd
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)   
app.secret_key = "thisismysecretkey&^@#$$*&788"
app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://root:root@localhost/testdb'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


@app.route('/', methods=['GET', 'POST'])
def main():
    if request.method == 'GET':
        flash("Get request sdfsdfdsf")
        return render_template("index.html")
    elif request.method == "POST":
        csv_filename = request.form.get("file")
        obj = csv_filename.split(".", 1)
        if obj[1] == 'csv':
            air_quality_sql = generate_sql_from_csv("/home/sunil/Downloads/" + csv_filename)
            with open(air_quality_sql, 'r') as sql:
                # con = sqlite3.connect(":memory:")
                # cur = con.cursor()
                # cur.executescript(sql.read())       
                con.commit()
                df = pd.read_sql("select * from `bristol-air-quality`;")
                print(df.info)
                return render_template("index.html")
        else:
            return render_template("index.html")
    else:
        return render_template("index.html")


def generate_sql_from_csv(csv_filename):
    sql_text = 'DROP TABLE IF EXISTS `bristol-air-quality`; CREATE TABLE `bristol-air-quality` (`Date Time` text, NOx real, NO2 real, NO real, SiteID INTEGER, PM10 real, NVPM10 real, VPM10 real, `NVPM2.5` real, `PM2.5` real, `VPM2.5` real, CO real, O3 real, SO2 real, Temperature real, RH real, `Air Pressure` real, Location text, geo_point_2d text, DateStart text, DateEnd text, Current text, `Instrument Type` text);'''
    with open(csv_filename) as csv_file:
        reader = csv.reader(csv_file, delimiter=';')
        next(reader)
        for row in reader:
            sql_text += f"INSERT INTO `bristol-air-quality` VALUES {tuple(row)};"
    output_sql_filename = f'{csv_filename.split(".")[0]}.sql'
    with open(output_sql_filename, 'w') as sql_file:
        sql_file.write(sql_text)
    return output_sql_filename


if __name__ == "__main__":
    app.run(debug=True)