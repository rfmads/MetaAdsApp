import mysql.connector

try:

    conn = mysql.connector.connect(
        host="82.29.180.15",
        port=3306,
        user="metaads",
        password="***",
        database="MetaAdsdb",

        connection_timeout=30,
        connect_timeout=30,
        read_timeout=60,
        write_timeout=60,

        autocommit=True,
        use_pure=True
    )   

    print("CONNECTED")
    conn.close()

except Exception as e:
    print("FAILED:", e)