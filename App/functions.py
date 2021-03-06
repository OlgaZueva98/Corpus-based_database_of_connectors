# -*- coding: utf-8 -*-

def to_Dataframe(pd, data_conn):
    data = []
    for d in data_conn:
        for t in d:
            data.append(tuple(t.split('|')))
    
    df = pd.DataFrame(data, columns=['Финский коннектор', 'Параметры', 'Финский контекст', 'Русский коннектор', 'Параметры', 'Русский контекст'])
    
    return df

def get_conn_data(mysql, request, pd, user_par = {}):
    conn = mysql.connect(host='localhost', user='root', password='database_hse', database='Connectors_DB')
    cursor = conn.cursor(buffered=True) 
    
    para = {}
    for k, v in user_par.items():
        if v != '""':
            para[k] = v.replace('"', '')
    
    s = """SELECT CONCAT_WS('|', FinConn, ParaFin, FinContext, RusConn, ParaRus, RusContext) as data_conn  
                                FROM (connectors_db.Document LEFT JOIN (connectors_db.Connector LEFT JOIN 
                                (connectors_db.Params LEFT JOIN connectors_db.Context ON Params.ID_Params = Context.ID_Params) 
                                ON Connector.ID_Connector = Params.ID_Connector) ON Document.ID_Document = Connector.ID_Document)
                                LEFT JOIN connectors_db.Author ON Document.ID_Author = Author.ID_Author
                                WHERE """
    for k, v in para.items():
        if len(para.keys())==1:
            s += '%s = "%s"' % (k, v) 
        else:
            s += '%s = "%s" AND ' % (k, v)

    if ' AND ' in s:
        s = s.strip(' AND ')
    
    
    cursor.execute(s)
    data_conn = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    df = to_Dataframe(pd, data_conn)
    
    names = {'Финский коннектор': 'Connector.FinConn', 'Русский коннектор': 'Connector.RusConn', 'Имя автора': 'Author.Name', 
             'Фамилия автора': 'Author.Surname', 'Название оригинала': 'Document.TitleOrigin', 
             'Название перевода': 'Document.TitleTranslation', 'Год создания': 'Document.YearCreation', 
             'Год перевода': 'Document.YearTrans', 'Жанр': 'Document.Genre'}
    
    for k, v in names.items():
        for key in para.keys():
            if key == v:
                para[k] = para.pop(key)
    
    return para, df

def get_by_context(mysql, request, pd, fincont, ruscont):
    conn = mysql.connect(host='localhost', user='root', password='database_hse', database='Connectors_DB')
    cursor = conn.cursor(buffered=True) 
    
    s = """SELECT CONCAT_WS('|', FinConn, ParaFin, FinContext, RusConn, ParaRus, RusContext) as data_conn  
                                FROM (connectors_db.Document LEFT JOIN (connectors_db.Connector LEFT JOIN 
                                (connectors_db.Params LEFT JOIN connectors_db.Context ON Params.ID_Params = Context.ID_Params) 
                                ON Connector.ID_Connector = Params.ID_Connector) ON Document.ID_Document = Connector.ID_Document)
                                LEFT JOIN connectors_db.Author ON Document.ID_Author = Author.ID_Author
                                WHERE """
    
    if fincont != '""' and ruscont == '""':
        s += """Context.FinContext LIKE '%{0}%'""".format(fincont.replace('"', ''))
        
    elif ruscont != '""' and fincont == '""':
        s += """Context.RusContext LIKE '%{0}%'""".format(ruscont.replace('"', ''))
    
    else:
        s += """Context.FinContext LIKE '%{0}%' AND Context.RusContext LIKE '%{1}%'""".format(fincont.replace('"', ''), ruscont.replace('"', ''))

    cursor.execute(s)
    data_conn = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    df = to_Dataframe(pd, data_conn)
    
    return fincont.replace('"', ''), ruscont.replace('"', ''), df
