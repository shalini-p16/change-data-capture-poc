import psycopg2
import argparse
from time import sleep
import random
from faker import Faker

fake = Faker()


def ensure_schema_and_tables():
    conn = psycopg2.connect(
        "dbname='commerce_db' user='cdc_user' host='postgres' password='cdc_password'"
    )
    curr = conn.cursor()
    curr.execute("CREATE SCHEMA IF NOT EXISTS commerce;")
    curr.execute("""
        CREATE TABLE IF NOT EXISTS commerce.users (
            id INT PRIMARY KEY,
            username TEXT,
            password TEXT
        );
    """)
    curr.execute("""
        CREATE TABLE IF NOT EXISTS commerce.products (
            id INT PRIMARY KEY,
            name TEXT,
            description TEXT,
            price NUMERIC
        );
    """)
    conn.commit()
    curr.close()
    conn.close()


def gen_user_product_data(num_records: int) -> None:
    ensure_schema_and_tables()
    for id in range(num_records):
        sleep(0.5)
        conn = psycopg2.connect(
            "dbname='commerce_db' user='cdc_user' host='postgres' password='cdc_password'"
        )
        curr = conn.cursor()
        curr.execute(
            "INSERT INTO commerce.users (id, username, password) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
            (id, fake.user_name(), fake.password()),
        )
        curr.execute(
            "INSERT INTO commerce.products (id, name, description, price) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (id, fake.name(), fake.text(), fake.random_int(min=1, max=100)),
        )
        conn.commit()

        # update 10 % of the time
        if random.randint(1, 100) >= 90:
            curr.execute(
                "UPDATE commerce.users SET username = %s WHERE id = %s",
                (fake.user_name(), id),
            )
            curr.execute(
                "UPDATE commerce.products SET name = %s WHERE id = %s",
                (fake.name(), id),
            )
            conn.commit()

        # delete 5 % of the time
        if random.randint(1, 100) >= 95:
            curr.execute("DELETE FROM commerce.users WHERE id = %s", (id,))
            curr.execute("DELETE FROM commerce.products WHERE id = %s", (id,))
            conn.commit()

        curr.close()
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-n",
        "--num_records",
        type=int,
        help="Number of records to generate",
        default=1000,
    )
    args = parser.parse_args()
    gen_user_product_data(args.num_records)
