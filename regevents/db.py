import psycopg2
import psycopg2.extras

CONNSTR= "dbname=[db name] user=[db user] password=[db pass] host=[db host]"


def run_query(query):
    with psycopg2.connect(CONNSTR) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()


def run_executemany(sql, data):
    with psycopg2.connect(CONNSTR) as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()

def create_reg_table():
    with psycopg2.connect(CONNSTR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS registrations 
                    (
                    id serial PRIMARY KEY, 
                    dt timestamp, 
                    code char(1), 
                    section varchar(255), 
                    regid varchar(32)),
                    event_time_sec integer;
                """
            )
        conn.commit()


def create_student_time_stats_table():
    with psycopg2.connect(CONNSTR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS student_time_stats
                    (
                    id serial PRIMARY KEY,
                    regid varchar(32),
                    median_time integer,
                    iqr_time integer,
                    mean_time integer,
                    lower_bound integer,
                    upper_bound integer,
                    event_counts integer,
                    min_time integer,
                    max_time integer
                    )
                """
            )
        conn.commit()


def create_adjacent_outliers_table():
    with psycopg2.connect(CONNSTR) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS adjacent_outliers
                    (
                    id serial PRIMARY KEY,
                    drop_id integer,
                    add_id integer,
                    drop_regid varchar(32),
                    add_regid varchar(32),
                    section varchar(255),
                    is_add_outlier boolean,
                    is_drop_outlier boolean
                    )
                """
            )
        conn.commit()

def add_event_time_column():
    # extract(hour from dt)*3600 + extract(minute from dt)*60 + extract(second from dt)
    pass



def bulk_insert_data(data_list):
    with psycopg2.connect(CONNSTR) as conn:
        with conn.cursor() as cur:
            insert_query = "INSERT INTO registrations (dt, code, section, regid) VALUES %s"
            psycopg2.extras.execute_values(cur, insert_query, data_list)
        conn.commit()


# create_adjacent_outliers_table()