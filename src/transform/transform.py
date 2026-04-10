import csv

vetorDados = []
vetorTratado = []

with open('data/dados.csv' , 'r', encoding='utf-8') as arquivo:
    leitura = csv.DictReader(arquivo)

    for linha in leitura:
        valores = list(linha.values())
        vetorDados.append(valores)

        dado_tratado = {
            'data_hora': linha['data_hora'],
            
            #Converter dados de disco
            'total_disco_gb': round(int(linha['total_disco']) / (1024 ** 3), 2),
            'disco_usado_gb': round(int(linha['disco_usado']) / (1024 ** 3), 2),
            'disco_livre_gb': round(int(linha['disco_livre']) / (1024 ** 3), 2),
            'disco_percentual': float(linha['disco_percentual']),

            #Converter dados de CPU
            'processador_nome': linha['processador_nome'],
            'nucleos_fiscos': linha['nucleos_fiscos'],
            'nucleos_totais': linha['nucleos_totais'],
            'frequencia_min_ghz': round(float(linha['frequencia_min']) / 1000, 2),
            'frequencia_max_ghz': round(float(linha['frequencia_max']) / 1000, 2),
            'frequencia_atual_ghz': round(float(linha['frequencia_atual']) / 1000, 2),
            'cpu_percentual': float(linha['cpu_percentual']),

            #Converter dados de RAM
            'ram_total_gb': round(float(linha['ram_total']) / (1024 ** 3),2),
            'ram_disponivel_gb':round(float(linha['ram_disponivel']) / (1024 ** 3),2),
            'ram_percentual': linha['ram_percentual'],

            #Converter os dados de rede
            'upload_mb': round(int(linha['upload']) / (1024 ** 2), 2),
            'download_mb': round(int(linha['download']) / (1024 ** 2), 2),

            'mac': linha['mac'],
            'ip': linha['ip']
        }
        vetorTratado.append(dado_tratado)

vetorProcessos = []
vProcessosTratados = []

with open('data/processos.csv' , 'r', encoding='utf-8') as arquivo:
    leitura = csv.DictReader(arquivo)

    for linha in leitura:
        valoresProcessos = list(linha.values())
        vetorProcessos.append(valoresProcessos)

        processosTratados = {
            "pid": linha['pid'],
            'usuario': linha['usuario'],
            'nomeProcesso': linha['nome'],
            'usoMemoriaProcesso': round(int(linha['memoria']) / (1024 ** 2), 2),
            'usoCpuProcesso': linha['uso_cpu'],
            'mac': linha['mac'],
            'ip': linha['ip']
        }
        vProcessosTratados.append(processosTratados)


colunas = list(vetorTratado[0].keys())

with open('dadosTratados.csv', 'w', newline='', encoding='utf-8') as arq:
    writer = csv.DictWriter(arq, fieldnames=colunas)
    writer.writeheader()
    writer.writerows(vetorTratado)


colunas = list(vProcessosTratados[0].keys())
with open('processosTratados.csv', 'w', newline='', encoding='utf-8') as arq:
    writer = csv.DictWriter(arq, fieldnames=colunas)
    writer.writeheader()
    writer.writerows(vProcessosTratados)