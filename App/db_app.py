# -*- coding: utf-8 -*-

from flask import Flask, url_for, render_template, request, redirect
import json
import pandas as pd
import mysql.connector as mysql
from functions import to_Dataframe, get_conn_data, get_by_context


app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    urls = {'Поиск': url_for('index'),
           'Контекстный поиск': url_for('context'),
           'results': url_for('results'),
           'context_results': url_for('context_results'),
           'Загрузка данных': url_for('file_load'),
            'Oops': url_for('oops')}

    return render_template('index.html', urls=urls)


@app.route('/results')
def results():
    user_par = {}
    
    user_par['Connector.FinConn'] = json.dumps(request.args['finconn'], ensure_ascii=False, indent = 4)
    user_par['Connector.RusConn'] = json.dumps(request.args['rusconn'], ensure_ascii=False, indent = 4)
    user_par['Author.Name'] = json.dumps(request.args['name'], ensure_ascii=False, indent = 4)
    user_par['Author.Surname'] = json.dumps(request.args['surname'], ensure_ascii=False, indent = 4)
    user_par['Document.TitleOrigin'] = json.dumps(request.args['origtitle'], ensure_ascii=False, indent = 4)
    user_par['Document.TitleTranslation'] = json.dumps(request.args['transtitle'], ensure_ascii=False, indent = 4)
    user_par['Document.YearCreation'] = json.dumps(request.args['yearcreat'], ensure_ascii=False, indent = 4)
    user_par['Document.YearTrans'] = json.dumps(request.args['yeartrans'], ensure_ascii=False, indent = 4)
    user_par['Document.Genre'] = json.dumps(request.args['genre'], ensure_ascii=False, indent = 4)

    para, df = None, None

    if all(value == '""' for value in user_par.values()):
        return render_template('oops.html')
    else:
        para, df = get_conn_data(mysql, request, pd, user_par)

        if request.form.getlist("write_in") != None:
            with open('conn_results.csv', 'w', encoding='utf-8') as f:
                df.to_csv(f, sep=';', index=False, encoding='utf-8')

    return render_template('results.html',  para = para, tables=[df.to_html(justify='center')], titles=df.columns.values)  


@app.route('/oops')
def oops():
    return render_template('oops.html')


@app.route('/context')
def context():
    return render_template('context.html')


@app.route('/context_results', methods=['POST'])
def context_results():
    fincont = json.dumps(request.form['fincont'], ensure_ascii=False, indent = 4)
    ruscont = json.dumps(request.form['ruscont'], ensure_ascii=False, indent = 4)

    fin, rus, df = None, None, None

    if fincont == None and ruscont == None:
        return render_template('oops.html')
    else:
        fin, rus, df = get_by_context(mysql, request, pd, fincont, ruscont)

        if request.form.getlist("write_in") != None:
            with open('context_results.csv', 'w', encoding='utf-8') as f:
                df.to_csv(f, sep=';', index=False, encoding='utf-8')

    return render_template('context_results.html',  fin = fin, rus = rus, tables=[df.to_html(justify='center')], titles=df.columns.values)


@app.route('/file_load')
def file_load():
    return render_template('file_load.html')


if __name__ == '__main__':
    app.run(debug=False)
