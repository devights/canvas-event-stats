from db import run_query, run_executemany


def get_outlier_registrations():
    # use 1.5 IQR to find outliers
    sql = """
        WITH quartiles AS (
            SELECT
                PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY event_time_sec) AS q1,
                PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY event_time_sec) AS q3
            FROM
                registrations
        ), iqr AS (
            SELECT
                q1,
                q3,
                q3 - q1 AS iqr
            FROM
                quartiles
        )
        SELECT
            *
        FROM
            registrations
        WHERE
            event_time_sec < (SELECT q1 - 1.5 * iqr FROM iqr) OR
            event_time_sec > (SELECT q3 + 1.5 * iqr FROM iqr)
    """


    sql = """
    select event_time_sec/3600, count(1) from registrations group by event_time_sec/3600;
    """
    run_query(sql)


def get_adjacent_registrations():
    sql = """
        SELECT
            r1.*,
            r2.*
        FROM
            registrations r1
        JOIN
            registrations r2
        ON
            r2.event_time_sec > r1.event_time_sec
            AND r2.event_time_sec < r1.event_time_sec + 60
            AND r2.section = r1.section
            AND r1.code = 'D'
            AND r2.code = 'A'
            AND r2.regid != r1.regid
    """
    output = run_query(sql)
    return output


def get_adjacent_registrations_by_regid(regid):
    sql = """
    -- Query 1
    SELECT
        r1.*,
        r2.*
    FROM
        registrations AS r1
    JOIN
        registrations AS r2
    ON
        r1.regid = '%s'
        AND r2.event_time_sec > r1.event_time_sec
        AND r2.event_time_sec < r1.event_time_sec + 60
        AND r2.section = r1.section
        AND r1.code = 'D'
        AND r2.code = 'A'
        AND r2.regid <> r1.regid
    
    UNION
    
    -- Query 2
    SELECT
        r1.*,
        r2.*
    FROM
        registrations AS r1
    JOIN
        registrations AS r2
    ON
        r2.regid = '%s'
        AND r2.event_time_sec > r1.event_time_sec
        AND r2.event_time_sec < r1.event_time_sec + 60
        AND r2.section = r1.section
        AND r1.code = 'D'
        AND r2.code = 'A'
        AND r2.regid <> r1.regid
    """ % (regid, regid)
    output = run_query(sql)
    return output




def build_student_time_stats():
    sql = """
        SELECT
            regid,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY event_time_sec) AS median_time,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY event_time_sec) AS q3_time,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY event_time_sec) AS q1_time,
            AVG(event_time_sec) AS mean_time,
            count(1) as event_count,
            min(event_time_sec) as min_time,
            max(event_time_sec) as max_time
        FROM
            registrations
        GROUP BY
            regid
    """
    output = run_query(sql)

    def _do_insert(data):
        sql = """
               INSERT INTO student_time_stats (regid, median_time, iqr_time, mean_time, lower_bound, upper_bound, event_counts, min_time, max_time)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               """
        run_executemany(sql, data)
    data = []
    for row in output:
        regid, median_time, q3_time, q1_time, mean_time, event_count, min_time, max_time = row
        iqr_time = q3_time - q1_time
        lower_bound = q1_time - 1.5 * iqr_time
        upper_bound = q3_time + 1.5 * iqr_time
        data.append((regid, median_time, iqr_time, mean_time, lower_bound, upper_bound, event_count, min_time, max_time))
        if len(data) == 10000:
            _do_insert(data)
            data = []
    if data:
        _do_insert(data)

def get_students_with_pattern():
    sql = """
        SELECT
            regid, lower_bound, upper_bound
        FROM
            student_time_stats
        WHERE
            event_counts > 10
            AND
            (upper_bound - lower_bound) < 24*60*60
            
                
    """
    output = run_query(sql)
    return output


def get_students_without_pattern():
    sql = """
        SELECT
            regid, lower_bound, upper_bound
        FROM
            student_time_stats
        WHERE
            event_counts > 10
            AND
            (upper_bound - lower_bound) > 24*60*60
    """
    output = run_query(sql)
    return output


def get_adjacent_outliers():
    students = get_students_with_pattern()

    def _insert_outliers(data):
        sql = """
               INSERT INTO adjacent_outliers (drop_id, drop_regid, add_id, add_regid, section, is_add_outlier, is_drop_outlier)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
        run_executemany(sql, data)
    for student in students:
        regid, lower_bound, upper_bound = student
        adjacent_reg = get_adjacent_registrations_by_regid(regid)
        data = []
        for reg in adjacent_reg:
            drop_id, drop_dt, drop_code, drop_section, drop_regid, \
                drop_event_time_sec, add_id, add_dt, add_code, add_section, \
                add_regid, add_event_time_sec = reg
            if regid == drop_regid:
                if drop_event_time_sec < lower_bound or \
                        drop_event_time_sec > upper_bound:
                    data.append((drop_id, drop_regid, add_id, add_regid,
                                 add_section, False, True))
            elif regid == add_regid:
                if add_event_time_sec < lower_bound or \
                        add_event_time_sec > upper_bound:
                    data.append((drop_id, drop_regid, add_id, add_regid,
                                 add_section, True, False))
        if data:
            _insert_outliers(data)
            data = []

def get_outlier_pairs():
    sql = """
        SELECT
            add_id,
            COUNT(DISTINCT drop_id) as distinct_drop_id_count
        FROM
            adjacent_outliers
        WHERE
            add_id IN (
                SELECT
                    add_id
                FROM
                    adjacent_outliers
                GROUP BY
                    add_id
                HAVING
                    COUNT(add_id) = 2
            )
        GROUP BY
            add_id
        HAVING
            COUNT(DISTINCT drop_id) > 1
    """
    output = run_query(sql)
    add_ids = [row[0] for row in output]
    return add_ids

def get_outlier_events():
    add_ids = get_outlier_pairs()
    sql = """
        SELECT 
            AO.*, 
            R1.dt as add_dt, 
            R2.dt as drop_dt
        FROM 
            adjacent_outliers AO
        JOIN 
            registrations R1 ON AO.add_id = R1.id
        JOIN 
            registrations R2 ON AO.drop_id = R2.id
        WHERE 
            AO.add_id IN (%s)
    """ % ", ".join([str(i) for i in add_ids])
    output = run_query(sql)
    rows = []
    for row in output:
        if row[8].date() == row[9].date():
            rows.append(row)
    return rows



if __name__ == "__main__":
    # get_outlier_registrations()
    # get_adjacent_registrations()
    # build_student_time_stats()
    # print(len(get_adjacent_outliers()))
    # print(len(get_students_with_pattern()))
    # print(len(get_students_without_pattern()))
    ev = get_outlier_events() # returns 32 students
    print(len(ev))

