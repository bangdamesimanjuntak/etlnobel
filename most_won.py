import sqlite3

def query_most_awarded_individual():
    conn = sqlite3.connect("nobel_prizes.db")
    cursor = conn.cursor()

    query = """
        SELECT
            l.knownName,
            COUNT(*) AS numAwards
        FROM
            Laureates AS l
        GROUP BY
            l.knownName
        ORDER BY
            numAwards DESC
        LIMIT 1;
    """

    cursor.execute(query)
    result = cursor.fetchone()

    conn.close()

    return result

most_awarded_individual = query_most_awarded_individual()
print(f"The individual who has won the most number of Nobel awards is: {most_awarded_individual[0]}")
