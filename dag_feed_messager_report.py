import telegram

import numpy as np
import pandas as pd
import pandahouse as ph

from datetime import date, timedelta, datetime

import matplotlib.pyplot as plt
import seaborn as sns
import io

from airflow.decorators import dag, task


connection = {...}  # данные для подключения к БД

# параметры по умолчанию
default_args = {
    'owner': 'g.turkiya',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 11, 25)
}

schedule_interval = '0 8 * * *'  # интервал запуска DAG (11:00 по московскому времени)

my_token = ...  # токен бота
bot = telegram.Bot(token=my_token) # получение доступа

chat_id = ...  # ID чата, в который будут отправляться отчеты


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_feed_messager_report():

    # извлечение данных для краткого отчета
    @task()
    def extract_feed_report():
        query_report = """
            SELECT
                uniq(user_id) AS DAU,
                countIf(action='view') AS views,
                countIf(action='like') AS likes,
                likes/views AS CTR
            FROM simulator_20250920.feed_actions
            WHERE toDate(time) = yesterday()
            GROUP BY toDate(time)
            """

        data_report = ph.read_clickhouse(query=query_report, connection=connection)
        return data_report
    
    
    @task()
    def extract_messager_report():
        query_report = """
            WITH
            send_data AS (
                SELECT
                    user_id,
                    COUNT(receiver_id) AS messages_sent
                FROM simulator_20250920.message_actions
                WHERE toDate(time) = yesterday()
                GROUP BY user_id
                )
            
            SELECT
                uniq(user_id) AS DAU,
                COUNT() AS total_messages_count,
                (SELECT AVG(messages_sent) FROM send_data) AS avg_messages_per_user
            FROM simulator_20250920.message_actions
            WHERE toDate(time) = yesterday()
            """

        data_report = ph.read_clickhouse(query=query_report, connection=connection)
        return data_report
    
    
    # извлечение данных для графиков
    @task()
    def extract_feed_graphs():        
        query_feed = """
            SELECT
                uniq(user_id) AS DAU,
                countIf(action='view') AS views,
                countIf(action='like') AS likes,
                likes/views AS CTR,
                toDate(time) AS date
            FROM simulator_20250920.feed_actions
            WHERE toDate(time) BETWEEN (today() - 7) AND yesterday()
            GROUP BY toDate(time)
            """
        
        data_feed = ph.read_clickhouse(query=query_feed, connection=connection)
        return data_feed
    
    
    @task()
    def extract_messager_graphs():
        query_messager = """
            WITH
            send_data AS (
                SELECT
                    uniq(user_id) AS users,
                    COUNT(user_id) AS messages_sent,
                    messages_sent / users AS avg_messages_per_user,
                    toDate(time) AS date
                FROM simulator_20250920.message_actions
                WHERE toDate(time) BETWEEN (today() - 7) AND yesterday()
                GROUP BY toDate(time)
                ),
            messager_data AS (
                SELECT
                    uniq(user_id) AS DAU,
                    COUNT(user_id) AS total_messages_count,
                    toDate(time) AS date
                FROM simulator_20250920.message_actions
                WHERE toDate(time) BETWEEN (today() - 7) AND yesterday()
                GROUP BY toDate(time)
                )

            SELECT
                messager_data.DAU AS DAU,
                messager_data.total_messages_count AS total_messages_count,
                send_data.avg_messages_per_user AS avg_messages_per_user,
                messager_data.date AS date
            FROM messager_data LEFT JOIN send_data USING(date)
            """

        data_messager = ph.read_clickhouse(query=query_messager, connection=connection)
        return data_messager
    
    
    # отправка краткого отчета за вчера
    @task
    def send_text_report(data_feed, data_messager):      
        message = f"Информация о метриках за вчера ({date.today() - timedelta(days=1)})\n\n" + \
        f"DAU ленты: {data_feed['DAU'].iloc[0]}\n" + \
        f"Количество просмотров: {data_feed['views'].iloc[0]}\nКоличество лайков: {data_feed['likes'].iloc[0]}\n" + \
        f"CTR: {data_feed['CTR'].iloc[0]: .4f}\n\n" + \
        f"DAU мессенджера: {data_messager['DAU'].iloc[0]}\n" + \
        f"Общее число сообщений: {data_messager['total_messages_count'].iloc[0]}\n" + \
        f"Среднее по пользователям число отправленных сообщений: {data_messager['avg_messages_per_user'].iloc[0]: .2f}"

        bot.sendMessage(chat_id=chat_id, text=message)

    
    # отправка графиков с динамикой метрик за последнюю неделю
    @task
    def send_graphs(data_feed, data_messager):
        # DAU
        plt.figure(figsize=(20, 10))
        sns.lineplot(x=data_feed['date'], y=data_feed['DAU'], label='DAU ленты новостей')
        sns.lineplot(x=data_messager['date'], y=data_messager['DAU'], label='DAU мессенджера')
        plt.title('Динамика DAU за последние 7 дней')
        plt.xlabel('Дата')
        plt.ylabel('DAU')
        plt.legend()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'DAU.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        # Остальные метрики
        fig, axes = plt.subplots(2, 2, figsize=(15, 10))

        # график 1: Лайки и просмотры
        sns.lineplot(x=data_feed['date'], y=data_feed['views'], ax=axes[0, 0], label='Просмотры')
        sns.lineplot(x=data_feed['date'], y=data_feed['likes'], ax=axes[0, 0], label='Лайки')
        axes[0, 0].set_title('Просмотры и лайки')
        axes[0, 0].set_xlabel('Дата')
        axes[0, 0].set_ylabel('Количество')

        # график 2: CTR
        sns.lineplot(x=data_feed['date'], y=data_feed['CTR'], ax=axes[1, 0])
        axes[1, 0].set_title('CTR')
        axes[1, 0].set_xlabel('Дата')
        axes[1, 0].set_ylabel('CTR')

        # график 3: Общее число сообщений
        sns.lineplot(x=data_messager['date'], y=data_messager['total_messages_count'], ax=axes[0, 1])
        axes[0, 1].set_title('Общее число сообщений')
        axes[0, 1].set_xlabel('Дата')
        axes[0, 1].set_ylabel('Количество')

        # график 3: Среднее по пользователям число отправленных сообщений
        sns.lineplot(x=data_messager['date'], y=data_messager['avg_messages_per_user'], ax=axes[1, 1])
        axes[1, 1].set_title('Среднее по пользователям число отправленных сообщений')
        axes[1, 1].set_xlabel('Дата')
        axes[1, 1].set_ylabel('Количество')

        plt.tight_layout()  # автоматический подгон отступов

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'metrics.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        

    # извлечение данных
    data_feed_report = extract_feed_report()
    data_messager_report = extract_messager_report()
    data_feed = extract_feed_graphs()
    data_messager = extract_messager_graphs()
    
    # отправка отчета
    send_text_report(data_feed_report, data_messager_report)
    send_graphs(data_feed, data_messager)


dag_feed_messager_report = dag_feed_messager_report()