import os, sys
import pyodbc

def main():
    if len(sys.argv) < 2:
        print('Usage: python run_sp.py "EXEC schema.proc;"')
        sys.exit(2)

    sql = sys.argv[1].strip()

    server=os.getenv("SERVER")
    db=os.getenv("DATABASE")
    uid=os.getenv("SQL_USERNAME")
    pwd=os.getenv("SQL_PASSWORD")
    driver=os.getenv("ODBC_DRIVER","ODBC Driver 18 for SQL Server")

    cs = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={db};"
        f"UID={uid};PWD={pwd};"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )

    cn = pyodbc.connect(cs, timeout=30)
    cn.autocommit = True
    cur = cn.cursor()
    cur.execute(sql)
    cn.close()
    print("OK:", sql)

if __name__ == "__main__":
    main()
