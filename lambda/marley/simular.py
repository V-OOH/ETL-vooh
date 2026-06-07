import csv
import random
from datetime import datetime, timedelta

# Configuração do cabeçalho baseado na arquitetura VOOH
header = [
    "data_hora", "idEmpresa", "idDisplay", "idZona", "total_disco_gb", "disco_usado_gb",
    "disco_livre_gb", "disco_percentual", "processador_nome", "nucleos_fiscos", "nucleos_totais",
    "frequencia_max_ghz", "frequencia_atual_ghz", "cpu_percentual", "ram_total_gb", "ram_disponivel_gb",
    "ram_percentual", "upload_mb", "download_mb", "errin", "dropin", "mtu", "latencia",
    "conn_established", "conn_listen", "conn_time_wait", "conn_close_wait", "conn_syn_sent", "mac", "ip", "boot_time"
]

data_inicio = datetime(2026, 5, 1, 0, 0)
total_minutos = 7 * 24 * 60  # 1 semana completa (10.080 registros)

with open('dados_instaveis_uma_semana.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(header)

    upload_acumulado = 1274.07
    download_acumulado = 3677.22

    for i in range(total_minutos):
        data_atual = data_inicio + timedelta(minutes=i)
        data_hora_str = data_atual.strftime("%d-%m-%Y %H:%M")

        # --- COMPORTAMENTO BASE NOMINAL (PADRÃO) ---
        cpu = round(random.uniform(8.0, 18.0), 1)
        ram_pct = round(random.uniform(70.0, 75.0), 1)
        latencia = round(random.uniform(3.0, 8.0), 1)
        conn_est = random.randint(20, 25)
        errin = random.randint(0, 2)
        dropin = random.randint(0, 1)
        freq_atual = round(random.uniform(2.5, 3.4), 1)

        # --- INJEÇÃO DE INSTABILIDADES PERIÓDICAS ---

        # 1. Todo dia no horário de pico (12:00 às 13:00) - Sobrecarga Térmica/CPU devido ao sol no totem
        if 12 <= data_atual.hour < 13:
            cpu = round(random.uniform(75.0, 92.0), 1)
            freq_atual = round(random.uniform(1.2, 1.8), 1)  # Thermal Throttling (processador desacelera)
            latencia = round(random.uniform(15.0, 45.0), 1)
            errin = random.randint(5, 15)

        # 2. Madrugada do Dia 03/05 (02:00 às 05:00) - Instabilidade Crítica de Sinal de Rede (Flutuação de Link)
        elif data_atual.date() == datetime(2026, 5, 3).date() and 2 <= data_atual.hour < 5:
            latencia = round(random.uniform(120.0, 850.0), 1) if random.random() > 0.1 else -1.0 # Perda de pacotes e jitter alto
            conn_est = random.randint(2, 8)
            errin = random.randint(40, 110)
            dropin = random.randint(30, 80)

        # 3. Dia 05/05 a partir das 18:00 até 22:00 - Memory Leak progressivo (Vazamento de Memória do Player)
        elif data_atual.date() == datetime(2026, 5, 5).date() and 18 <= data_atual.hour < 22:
            # A memória vai subindo de forma agressiva conforme o tempo passa nas 4 horas
            minutos_decorridos = (data_atual.hour - 18) * 60 + data_atual.minute
            fator_vazamento = (minutos_decorridos / 240) * 23.0  # Sobe até +23%
            ram_pct = round(random.uniform(72.0, 75.0) + fator_vazamento, 1)
            cpu = round(random.uniform(40.0, 65.0), 1)
            if ram_pct > 95:
                conn_est = random.randint(5, 10) # Sistema começa a travar sockets

        # Simulação de tráfego de rede contínuo
        upload_acumulado += round(random.uniform(0.01, 0.05), 2)
        download_acumulado += round(random.uniform(0.05, 0.25), 2)
        ram_disponivel = round(32.0 * (1 - (ram_pct / 100)), 2)

        row = [
            data_hora_str, 1, 1, 1, 512.0, 180.0, 332.0, 35.2,
            "AMD Ryzen 7 5700G", 8, 16, 4.6, freq_atual, cpu,
            32.0, ram_disponivel, ram_pct, round(upload_acumulado, 2),
            round(download_acumulado, 2), errin, dropin, 1500, latencia,
            conn_est, 31, 5, 5, 1, "E0:2B:E9:6D:2E:01", "192.168.1.10", "3, 13, 34"
        ]
        writer.writerow(row)

print("CSV com instabilidades cíclicas gerado com sucesso!")
