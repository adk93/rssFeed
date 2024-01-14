# Standard library imports
import requests
import pendulum
from typing import List, Tuple

# Third party imports
from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import xmltodict

RSS_FEEDS = {
    "RESULTS": "6639",
    "EBI": "6612",
    "ESPI": "6614",
    "INFO": "6600",
    "LISTINGS": "6616"
}


@dag(
    dag_id="rss_feed",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 13),
    catchup=False
)
def get_rss_feed() -> None:

    # Create our databases
    create_database = SQLExecuteQueryOperator(
        task_id="create_sql_table",
        conn_id="feed",
        sql=f"""
            CREATE TABLE IF NOT EXISTS feed (
                pubDate DATETIME PRIMARY KEY,
                link TEXT,
                company TEXT,
                topic TEXT,
                title TEXT,
                content TEXT,
                type TEXT
                )
            """
            )

    @task()
    def get_data(feed_number: str) -> List:
        www = f"https://biznes.pap.pl/pl/rss/{feed_number}"

        r = requests.get(www)
        feed = xmltodict.parse(r.text)
        return feed.get("rss", {}).get("channel", {}).get("item", [])

    @task()
    def load_data(item_list: List, feed_type: str) -> None:
        hook = SqliteHook(sqlite_conn_id='feed')

        stored = hook.get_pandas_df("SELECT * FROM feed;")

        new_info = []
        for item in item_list:
            if item['pubDate'] not in stored['pubDate'].values:
                company, topic = extract_company_topic(item['title'])
                new_info.append([item.get('pubDate', "N/A"),
                                 item.get('link', "N/A"),
                                 company,
                                 topic,
                                 item.get('title', "N/A"),
                                 item.get('content:encoded', "N/A"),
                                 feed_type])

        hook.insert_rows(table='feed', rows=new_info,
                         target_fields=['pubDate', 'link', 'company', 'topic', 'title', 'content', 'type'])

    for feed_type, feed_number in RSS_FEEDS.items():
        feed_data = get_data(feed_number)
        load_data(feed_data, feed_type)

    create_database.set_downstream(feed_data)


def extract_company_topic(title: str) -> Tuple[str, str]:
    character_position = title.find(")")

    if character_position == -1:
        company = title
        topic = title
    else:
        company = title[:character_position+1]
        topic = title[character_position+2:]

    return company, topic



get_rss_feed()


