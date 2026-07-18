import psycopg

connection_details = {
    "host": "localhost",
    "port": 5432,
    "dbname": "allianz",
    "user": "postgres",
    "password": "amit",
}


def main() -> None:
    try:
        with psycopg.connect(**connection_details) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT version();")
                result = cursor.fetchone()

                print("Connected successfully")
                print(result[0])

    except psycopg.Error as error:
        print(f"PostgreSQL connection failed: {error}")


if __name__ == "__main__":
    main()
