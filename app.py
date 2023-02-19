from flask import Flask, jsonify
from flask import request

from sqlalchemy import create_engine

from reflect import Reflect

url = "postgresql+psycopg2://postgres:mysecretpassword@localhost:5432/postgres"

engine = create_engine(url)
reflection = Reflect(engine, schema="public")

app = Flask(__name__)


@app.route("/tables", methods=["GET"])
def list_tables():
    result = reflection.list_tables()
    return jsonify(result.dict())


@app.route("/tables/<string:table_name>", methods=["GET"])
def table_paginator(table_name):
    print(request.path)
    page_number = request.args.get("page_number", 0, int)
    per_page = request.args.get("per_page", 100, int)
    result = reflection.paginate(table_name, page_number, per_page)
    return jsonify(result.dict())
