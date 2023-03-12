from flask import Flask, jsonify, request
from sqlalchemy import MetaData, create_engine
from sqlalchemy.orm import Session

from reflect import Reflect

url = "postgresql+psycopg2://postgres:mysecretpassword@localhost:5432/postgres"
schema = "public"

engine = create_engine(url)
default_reflection = Reflect(engine, schema="public")

app = Flask(__name__)


@app.route("/tables", methods=["GET"])
def list_tables():
    result = default_reflection.list_tables()
    return jsonify(result.dict())


@app.route("/tables/<string:table_name>", methods=["GET"])
def table_paginator(table_name):
    page_number = request.args.get("page_number", 0, int)
    per_page = request.args.get("per_page", 100, int)
    # TODO
    # column_names = request.args.getlist("column_names")
    result = default_reflection.paginate(
        table_name, page_number, per_page, column_names=[]
    )
    return jsonify(result.dict())
