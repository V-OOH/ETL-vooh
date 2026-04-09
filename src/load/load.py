import mysql.connector

conexao = mysql.connector.connect(
    host='localhost',
    user='seu_usuario',
    password='sua_senha',
    database='nome_do_banco'
)