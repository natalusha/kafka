from time import sleep
import requests
from bs4 import BeautifulSoup
from kafka import KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding="utf-8")
        value_bytes = bytes(value, encoding="utf-8")
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print("Message published successfully.")
    except Exception as ex:
        print("Exception in publishing message")
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(
            bootstrap_servers=["localhost:9092"], api_version=(0, 10)
        )
    except Exception as ex:
        print("Exception while connecting Kafka")
        print(str(ex))
    finally:
        return _producer


def fetch_raw(item_url):
    html = None
    print("Processing..{}".format(item_url))
    try:
        r = requests.get(item_url, headers=headers)
        if r.status_code == 200:
            html = r.text
    except Exception as ex:
        print("Exception while accessing raw html")
        print(str(ex))
    finally:
        return html.strip()


def get_clothes():
    clothes_list = []
    url = "https://byme.ua/en/clothes/dresses,-blouse/"
    print("Accessing list")

    try:
        r = requests.get(url, headers=headers)
        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, "lxml")
            links = soup.select("p.byme-name-h4 a")
            idx = 0
            for link in links:

                sleep(2)
                item = fetch_raw(link["href"])
                clothes_list.append(item)
                idx += 1
    except Exception as ex:
        print("Exception in get_clothes")
        print(str(ex))
    finally:
        return clothes_list


if __name__ == "__main__":
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36",
        "Pragma": "no-cache",
    }

    all_items = get_clothes()
    if len(all_items) > 0:
        kafka_producer = connect_kafka_producer()
        for recipe in all_items:
            publish_message(kafka_producer, "raw_clothes", "raw", recipe.strip())
        if kafka_producer is not None:
            kafka_producer.close()
